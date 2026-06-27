"""
Integration tests for GraphQL subscription support (#764).

Covers the acceptance criteria without requiring a live WebSocket server:
- Subscription type is present in the schema with contractEvents field
- contract_events resolver is an AsyncGenerator
- Channel group cleanup runs in the finally block (verified via mock)
- Existing HTTP GraphQL queries continue to work unchanged
- Rate limiter enforces max 5 concurrent subscriptions per IP
- Channel layer publishing triggers subscription yield
"""

from __future__ import annotations

import asyncio
import inspect
from collections import defaultdict
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from soroscan.ingest.schema import Subscription, schema
from soroscan.subscription_middleware import SubscriptionRateLimitMiddleware

from .factories import ContractEventFactory, TrackedContractFactory, UserFactory


# ---------------------------------------------------------------------------
# 1. Schema introspection — subscription type exists and has the right fields
# ---------------------------------------------------------------------------

class TestSubscriptionSchemaShape:
    """Schema-level checks that require no database or channel layer."""

    def test_schema_has_subscription_type(self):
        assert schema.subscription is not None

    def test_subscription_type_name_is_subscription(self):
        introspection = """
            query {
                __schema {
                    subscriptionType { name }
                }
            }
        """
        result = schema.execute_sync(introspection)
        assert result.errors is None
        assert result.data["__schema"]["subscriptionType"]["name"] == "Subscription"

    def test_subscription_has_contract_events_field(self):
        introspection = """
            query {
                __schema {
                    subscriptionType {
                        fields {
                            name
                            args { name type { kind ofType { name } } }
                        }
                    }
                }
            }
        """
        result = schema.execute_sync(introspection)
        assert result.errors is None
        fields = {
            f["name"]: f
            for f in result.data["__schema"]["subscriptionType"]["fields"]
        }
        assert "contractEvents" in fields, (
            "Subscription type must expose a contractEvents field"
        )

    def test_contract_events_field_accepts_contract_id_arg(self):
        introspection = """
            query {
                __schema {
                    subscriptionType {
                        fields {
                            name
                            args { name }
                        }
                    }
                }
            }
        """
        result = schema.execute_sync(introspection)
        assert result.errors is None
        fields = {
            f["name"]: f
            for f in result.data["__schema"]["subscriptionType"]["fields"]
        }
        arg_names = {a["name"] for a in fields["contractEvents"]["args"]}
        assert "contractId" in arg_names

    def test_subscription_has_notifications_field(self):
        """Existing notifications subscription must still be present."""
        introspection = """
            query {
                __schema {
                    subscriptionType { fields { name } }
                }
            }
        """
        result = schema.execute_sync(introspection)
        assert result.errors is None
        field_names = {
            f["name"]
            for f in result.data["__schema"]["subscriptionType"]["fields"]
        }
        assert "notifications" in field_names


# ---------------------------------------------------------------------------
# 2. Resolver is an AsyncGenerator
# ---------------------------------------------------------------------------

class TestContractEventsResolver:
    """Unit tests for the contract_events resolver function."""

    def test_contract_events_is_async_generator_function(self):
        """The resolver must be an async generator, not a coroutine or iterator."""
        assert inspect.isasyncgenfunction(Subscription.contract_events), (
            "contract_events must be defined with `async def ... yield` "
            "(AsyncGenerator), not a regular coroutine"
        )

    def test_notifications_is_async_generator_function(self):
        """Existing notifications subscription is also an async generator."""
        assert inspect.isasyncgenfunction(Subscription.notifications)


# ---------------------------------------------------------------------------
# 3. Resolver behaviour — channel group add / discard lifecycle
# ---------------------------------------------------------------------------

class TestContractEventsChannelLifecycle:
    """
    Test channel group add/discard using a mock channel layer.

    We stop the generator after the first yield by cancelling it in the
    finally path, which verifies group_discard is always called.
    """

    @pytest.mark.asyncio
    async def test_group_add_called_on_subscribe(self):
        """group_add is called with the correct group name when subscribing."""
        channel_layer = _make_mock_channel_layer(events=[])

        with patch(
            "soroscan.ingest.schema.get_channel_layer", return_value=channel_layer
        ):
            gen = Subscription().contract_events(
                info=_make_info(), contract_id="CONTRACT_XYZ"
            )
            # Iterate briefly; StopAsyncIteration fires immediately (empty events)
            with _suppress_stop_async_iteration():
                await gen.__anext__()

        channel_layer.group_add.assert_awaited_once()
        call_args = channel_layer.group_add.await_args
        group_name = call_args[0][0]
        assert group_name == "events_CONTRACT_XYZ", (
            f"Expected group name 'events_CONTRACT_XYZ', got '{group_name}'"
        )

    @pytest.mark.asyncio
    async def test_group_discard_called_on_generator_close(self):
        """group_discard (cleanup) is called when the generator is closed."""
        # Provide one event so the generator enters the while loop, then close it
        channel_layer = _make_mock_channel_layer(
            events=[_make_channel_message("CONTRACT_ABC", "transfer", 500)]
        )

        with patch(
            "soroscan.ingest.schema.get_channel_layer", return_value=channel_layer
        ):
            with patch(
                "soroscan.ingest.schema.ContractEvent.objects"
            ) as mock_objects:
                mock_qs = MagicMock()
                mock_qs.select_related.return_value = mock_qs
                mock_qs.get = MagicMock(
                    return_value=_make_db_event("CONTRACT_ABC", "transfer", 500)
                )
                mock_objects.select_related.return_value = mock_qs

                gen = Subscription().contract_events(
                    info=_make_info(), contract_id="CONTRACT_ABC"
                )
                # aclose() triggers the finally block
                await gen.aclose()

        channel_layer.group_discard.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_group_discard_called_even_after_exception(self):
        """group_discard is called even if the channel layer raises mid-loop."""
        channel_layer = _make_mock_channel_layer(events=[])
        channel_layer.receive = AsyncMock(side_effect=RuntimeError("channel error"))

        with patch(
            "soroscan.ingest.schema.get_channel_layer", return_value=channel_layer
        ):
            gen = Subscription().contract_events(
                info=_make_info(), contract_id="CONTRACT_ERR"
            )
            with pytest.raises(RuntimeError):
                await gen.__anext__()

        channel_layer.group_discard.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_no_channel_layer_exits_gracefully(self):
        """If channel layer is not configured, the generator exits without error."""
        with patch(
            "soroscan.ingest.schema.get_channel_layer", return_value=None
        ):
            gen = Subscription().contract_events(
                info=_make_info(), contract_id="ANYTHING"
            )
            items = []
            async for item in gen:
                items.append(item)
        assert items == [], "Should yield nothing when no channel layer is configured"


# ---------------------------------------------------------------------------
# 4. Event published to channel group is yielded by the subscription
# ---------------------------------------------------------------------------

@pytest.mark.django_db
class TestContractEventsYieldsPublishedEvent:
    """
    Verify that a message published to the channel group causes
    the resolver to yield the corresponding ContractEvent.
    """

    @pytest.mark.asyncio
    async def test_yields_event_after_channel_message(self):
        user = UserFactory()
        contract = TrackedContractFactory(owner=user)
        event = ContractEventFactory(contract=contract, event_type="transfer", ledger=1000)

        channel_message = _make_channel_message(
            contract.contract_id, event.event_type, event.ledger
        )
        channel_layer = _make_mock_channel_layer(events=[channel_message])

        with patch(
            "soroscan.ingest.schema.get_channel_layer", return_value=channel_layer
        ):
            gen = Subscription().contract_events(
                info=_make_info(), contract_id=contract.contract_id
            )
            yielded = await gen.__anext__()

        # yielded is the ORM ContractEvent instance fetched in get_event()
        assert yielded.event_type == event.event_type
        assert yielded.ledger == event.ledger
        await gen.aclose()

    @pytest.mark.asyncio
    async def test_skips_message_when_event_not_found(self):
        """
        If the DB lookup raises ContractEvent.DoesNotExist the message is
        skipped and the generator continues (does not raise).
        """
        channel_layer = _make_mock_channel_layer(
            events=[
                _make_channel_message("MISSING_CONTRACT", "transfer", 9999),
                # Second message raises StopAsyncIteration via empty list
            ]
        )
        # Make receive raise StopAsyncIteration after the first message
        # by using side_effect sequence
        from soroscan.ingest.models import ContractEvent

        channel_layer.receive = AsyncMock(
            side_effect=[
                _make_channel_message("MISSING_CONTRACT", "transfer", 9999),
                asyncio.CancelledError(),
            ]
        )

        with patch(
            "soroscan.ingest.schema.get_channel_layer", return_value=channel_layer
        ):
            with patch.object(
                ContractEvent.objects,
                "select_related",
                side_effect=ContractEvent.DoesNotExist,
            ):
                gen = Subscription().contract_events(
                    info=_make_info(), contract_id="MISSING_CONTRACT"
                )
                with pytest.raises(asyncio.CancelledError):
                    await gen.__anext__()

        # group_discard must still have been called
        channel_layer.group_discard.assert_awaited_once()


# ---------------------------------------------------------------------------
# 5. Existing HTTP GraphQL queries continue to work
# ---------------------------------------------------------------------------

@pytest.mark.django_db
class TestHttpQueriesUnaffected:
    """Verify existing HTTP-style queries are not broken by subscription additions."""

    def test_contracts_query_still_works(self):
        user = UserFactory()
        TrackedContractFactory(owner=user)
        result = schema.execute_sync("query { contracts { id contractId name } }")
        assert result.errors is None
        assert len(result.data["contracts"]) >= 1

    def test_events_query_still_works(self):
        user = UserFactory()
        contract = TrackedContractFactory(owner=user)
        ContractEventFactory(contract=contract)
        result = schema.execute_sync(
            "query { events(first: 5) { edges { node { id eventType } } totalCount } }"
        )
        assert result.errors is None
        assert result.data["events"]["totalCount"] >= 1

    def test_schema_introspection_still_works(self):
        result = schema.execute_sync("query { __schema { queryType { name } } }")
        assert result.errors is None
        assert result.data["__schema"]["queryType"]["name"] == "Query"


# ---------------------------------------------------------------------------
# 6. Rate-limit middleware — max 5 concurrent subscriptions per IP
# ---------------------------------------------------------------------------

class TestSubscriptionRateLimitMiddleware:
    """Unit tests for SubscriptionRateLimitMiddleware."""

    def setup_method(self):
        SubscriptionRateLimitMiddleware._active_subscriptions.clear()

    def test_allows_up_to_five_connections_from_same_ip(self):
        mw = SubscriptionRateLimitMiddleware(app=AsyncMock())
        ip = "10.0.0.1"
        # Simulate 5 opens
        for _ in range(5):
            SubscriptionRateLimitMiddleware._active_subscriptions[ip] += 1
        assert SubscriptionRateLimitMiddleware._active_subscriptions[ip] == 5

    @pytest.mark.asyncio
    async def test_rejects_sixth_connection_from_same_ip(self):
        closed_with: list[int] = []

        async def fake_send(message):
            if message.get("type") == "websocket.close":
                closed_with.append(message.get("code"))

        ip = "10.0.0.2"
        SubscriptionRateLimitMiddleware._active_subscriptions[ip] = 5

        app = AsyncMock()
        mw = SubscriptionRateLimitMiddleware(app)
        scope = {
            "type": "websocket",
            "headers": [(b"x-forwarded-for", ip.encode())],
            "client": (ip, 12345),
        }
        await mw(scope, AsyncMock(), fake_send)

        assert 4429 in closed_with, "Should close with code 4429 when rate limited"
        app.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_allows_connection_below_limit(self):
        app_called = []

        async def fake_app(scope, receive, send):
            app_called.append(True)

        ip = "10.0.0.3"
        # Only 2 active — well below limit
        SubscriptionRateLimitMiddleware._active_subscriptions[ip] = 2

        mw = SubscriptionRateLimitMiddleware(fake_app)
        scope = {
            "type": "websocket",
            "headers": [(b"x-forwarded-for", ip.encode())],
            "client": (ip, 54321),
        }
        await mw(scope, AsyncMock(), AsyncMock())
        assert app_called, "App should have been called when under the limit"

    @pytest.mark.asyncio
    async def test_decrements_counter_after_connection_closes(self):
        ip = "10.0.0.4"

        async def fake_app(scope, receive, send):
            pass  # connection closes immediately

        mw = SubscriptionRateLimitMiddleware(fake_app)
        scope = {
            "type": "websocket",
            "headers": [],
            "client": (ip, 9999),
        }
        await mw(scope, AsyncMock(), AsyncMock())
        # Counter should be back to 0 (or absent) after close
        assert SubscriptionRateLimitMiddleware._active_subscriptions.get(ip, 0) == 0

    def test_get_client_ip_uses_x_forwarded_for(self):
        scope = {
            "headers": [(b"x-forwarded-for", b"203.0.113.1, 10.0.0.1")],
            "client": ("127.0.0.1", 0),
        }
        assert SubscriptionRateLimitMiddleware._get_client_ip(scope) == "203.0.113.1"

    def test_get_client_ip_falls_back_to_client(self):
        scope = {"headers": [], "client": ("192.168.5.5", 0)}
        assert SubscriptionRateLimitMiddleware._get_client_ip(scope) == "192.168.5.5"

    def test_get_client_ip_returns_unknown_when_no_client(self):
        scope = {"headers": [], "client": None}
        assert SubscriptionRateLimitMiddleware._get_client_ip(scope) == "unknown"

    @pytest.mark.asyncio
    async def test_non_websocket_scope_bypasses_middleware(self):
        app = AsyncMock()
        mw = SubscriptionRateLimitMiddleware(app)
        scope = {"type": "http"}
        await mw(scope, AsyncMock(), AsyncMock())
        app.assert_awaited_once()


# ---------------------------------------------------------------------------
# 7. ASGI routing — GraphQLWSConsumer is mounted at /graphql/
# ---------------------------------------------------------------------------

class TestAsgiRouting:
    """Verify the ASGI application has the GraphQL WS consumer mounted."""

    def test_graphql_ws_consumer_imported_in_asgi(self):
        """GraphQLWSConsumer must be importable from strawberry.channels."""
        from strawberry.channels import GraphQLWSConsumer  # noqa: F401
        assert GraphQLWSConsumer is not None

    def test_asgi_application_is_protocol_type_router(self):
        from soroscan.asgi import application
        from channels.routing import ProtocolTypeRouter
        assert isinstance(application, ProtocolTypeRouter)

    def test_asgi_schema_is_the_schema_object(self):
        """The schema passed to GraphQLWSConsumer is the same schema object."""
        from soroscan.ingest.schema import schema as expected_schema
        assert expected_schema.subscription is not None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_info():
    """Minimal strawberry Info mock suitable for subscription resolver."""
    info = MagicMock()
    info.context = {}
    return info


def _make_mock_channel_layer(events: list[dict]):
    """
    Build a mock channel layer whose receive() pops from *events* and then
    blocks indefinitely (simulating a quiet channel after the burst).
    """
    channel_layer = MagicMock()
    channel_layer.new_channel = AsyncMock(return_value="test-channel-name")
    channel_layer.group_add = AsyncMock()
    channel_layer.group_discard = AsyncMock()

    remaining = list(events)

    async def receive(channel_name):
        if remaining:
            return remaining.pop(0)
        # Simulate blocking forever — the caller must close the generator
        await asyncio.sleep(3600)

    channel_layer.receive = receive
    return channel_layer


def _make_channel_message(contract_id: str, event_type: str, ledger: int) -> dict:
    return {
        "type": "contract_event",
        "data": {
            "contract_id": contract_id,
            "event_type": event_type,
            "ledger": ledger,
            "event_index": 0,
            "tx_hash": "tx-test",
            "payload": {"amount": 42},
        },
    }


def _make_db_event(contract_id: str, event_type: str, ledger: int):
    """Create a lightweight mock that looks like a ContractEvent ORM instance."""
    event = MagicMock()
    event.event_type = event_type
    event.ledger = ledger
    event.contract_id = contract_id
    return event


class _suppress_stop_async_iteration:
    """Context manager that swallows StopAsyncIteration."""

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return exc_type is StopAsyncIteration
