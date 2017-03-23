# -*- coding: utf-8 -*-
# Copyright 2017 Vector Creations Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""This module contains the implementation of both the client and server
protocols.

The basic structure of the protocol is line based, where the initial word of
each line specifies the command. The rest of the line is parsed based on the
command. For example, the `RDATA` command is defined as::

    RDATA <stream_name> <token> <row_json>

(Note that `<row_json>` may contains spaces, but cannot contain newlines.)

Blank lines are ignored.


# Keep alives

Both sides are expected to send at least one command every 5s (`PING_TIME`), and
should send a `PING` command if necessary. If either side do not receive a
command within e.g. 15s then the connection should be closed.

Because the server may be connected to manually using e.g. netcat, the timeouts
aren't enabled until an initial `PING` command is seen. Both the client and
server implementations below send a `PING` command immediately on connection to
ensure the timeouts are enabled.

This ensures that both sides can quickly realize if the tcp connection has gone
and handle the situation appropriately.


# Start up

When a new connection is made, the server:
 * Sends a `SERVER` command, which includes the identity of the server, allowing
   the client to detect if its connected to the expected server
 * Sends a `PING` command as above, to enable the client to time out connections
   promptly.

The client:
 * Sends a `NAME` command, allowing the server to associate a human friendly
   name with the connection. This is optional.
 * Sends a `PING` as above
 * For each stream the client wishes to subscribe to it sends a `REPLICATE`
   with the stream_name and token it wants to subscribe from.
 * On receipt of a `SERVER` command, checks that the server name matches the
   expected server name.


# Error handling

If either side detects an error it can send an `ERROR` command and close the
connection.

If the client side loses the connection to the server it should reconnect,
following the steps above.


# Reliability

In general the replication stream should be consisdered an unreliable transport
since e.g. commands are not resent if the connection disappears.

The exception to that are the replication streams, i.e. RDATA commands, since
theses include tokens which can be used to restart the stream on connection
errors.

The client should keep track of the token in the last RDATA command received
for each stream so that on reconneciton it can start streaming from the correct
place. Note: not all RDATA have valid tokens due to batching. See
`commands.RdataCommand` for more details.


# Example

An example iteraction is shown below. Each line is prefixed with '>' or '<' to
indicate which side is sending, these are *not* included on the wire::

    * connection established *
    > SERVER localhost:8823
    > PING 1490197665618
    < NAME synapse.app.appservice
    < PING 1490197665618
    < REPLICATE events 1
    < REPLICATE backfill 1
    < REPLICATE caches 1
    > POSITION events 1
    > POSITION backfill 1
    > POSITION caches 1
    > RDATA caches 2 ["get_user_by_id",["@01register-user:localhost:8823"],1490197670513]
    > RDATA events 14 ["$149019767112vOHxz:localhost:8823",
        "!AFDCvgApUmpdfVjIXm:localhost:8823","m.room.guest_access","",null]
    < PING 1490197675618
    > ERROR server stopping
    * connection closed by server *

The `POSITION` command sent by the server is used to set the clients position
without needing to send data with the `RDATA` command.
"""

from twisted.internet import defer
from twisted.protocols.basic import LineOnlyReceiver

from commands import (
    COMMAND_MAP, VALID_CLIENT_COMMANDS, VALID_SERVER_COMMANDS,
    ErrorCommand, ServerCommand, RdataCommand, PositionCommand, PingCommand,
    NameCommand, ReplicateCommand, UserSyncCommand, SyncCommand,
)
from streams import STREAMS_MAP

import logging
import synapse.metrics


metrics = synapse.metrics.get_metrics_for(__name__)

inbound_commands_counter = metrics.register_counter(
    "inbound_commands", labels=["command"],
)
outbound_commands_counter = metrics.register_counter(
    "outbound_commands", labels=["command"],
)

logger = logging.getLogger(__name__)


PING_TIME = 5000


next_conn_id = 1


class BaseReplicationStreamProtocol(LineOnlyReceiver):
    """Base replication protocol shared between client and server.

    Reads lines (ignoring blank ones) and parses them into command classes,
    asserting that they are valid for the given direction, i.e. server commands
    are only sent by the server.

    On receiving a new command it calls `on_<COMMAND_NAME>` with the parsed
    command.

    It also sends `PING` periodically, and correctly times out remote connections
    (if they send a `PING` command)
    """
    delimiter = b'\n'

    VALID_INBOUND_COMMANDS = []  # Valid commands we expect to receive
    VALID_OUTBOUND_COMMANDS = []  # Valid commans we can send

    def __init__(self, clock):
        self.clock = clock

        self.last_received_command = self.clock.time_msec()
        self.last_sent_command = 0

        self.received_ping = False

        self.connection_established = False

        # List of pending commands to send once we've established the connection
        self.pending_commands = []

    def connectionMade(self):
        self.connection_established = True

        # Send any pending commands
        for cmd in self.pending_commands:
            self.send_command(cmd)

        self.pending_commands = []

        # Starts sending pings
        self.clock.looping_call(self.send_ping, 5000)

        # Always send the initial PING so that the other side knows that they
        # can time us out.
        self.send_command(PingCommand(self.clock.time_msec()))

    def send_ping(self):
        """Periodically sends a ping and checks if we should close the connection
        due to the other side timing out.
        """
        now = self.clock.time_msec()
        if now - self.last_sent_command >= PING_TIME:
            self.send_command(PingCommand(now))

        if self.received_ping and now - self.last_received_command > PING_TIME * 3:
            logger.info(
                "Connection hasn't received command in %r ms. Closing.",
                now - self.last_received_command
            )
            self.send_error("ping timeout")

    def lineReceived(self, line):
        """Called when we've received a line
        """
        if line.strip() == "":
            # Ignore blank lines
            return

        line = line.decode("utf-8")
        cmd_name, rest_of_line = line.split(" ", 1)

        if cmd_name not in self.VALID_INBOUND_COMMANDS:
            self.send_error("invalid command: %s", cmd_name)
            return

        self.last_received_command = self.clock.time_msec()

        inbound_commands_counter.inc(cmd_name)

        cmd_cls = COMMAND_MAP[cmd_name]
        try:
            cmd = cmd_cls.from_line(rest_of_line)
        except Exception as e:
            logger.exception("failed to parse line %r: %r", cmd_name, rest_of_line)
            self.send_error(
                "failed to parse line for  %r: %r (%r):" % (cmd_name, e, rest_of_line)
            )
            return

        try:
            getattr(self, "on_%s" % (cmd_name,))(cmd)
        except Exception:
            logger.exception("Failed to handle line: %r", line)

    def send_error(self, error_string, *args):
        """Send an error to remote and close the connection.
        """
        self.send_command(ErrorCommand(error_string % args))
        self.transport.loseConnection()

    def send_command(self, cmd):
        if not self.connection_established:
            logger.info("Queing as conn not ready %r", cmd)
            self.pending_commands.append(cmd)
            return

        outbound_commands_counter.inc(cmd.NAME)

        string = "%s %s" % (cmd.NAME, cmd.to_line(),)
        if "\n" in string:
            raise Exception("Unexpected newline in command: %r", string)

        self.sendLine(string.encode("utf-8"))

        self.last_sent_command = self.clock.time_msec()

    def on_PING(self, line):
        self.received_ping = True

    def on_ERROR(self, cmd):
        logger.error("Remote reported error: %r", cmd.data)


class ServerReplicationStreamProtocol(BaseReplicationStreamProtocol):
    VALID_INBOUND_COMMANDS = VALID_CLIENT_COMMANDS
    VALID_OUTBOUND_COMMANDS = VALID_SERVER_COMMANDS

    def __init__(self, server_name, clock, streamer, addr):
        BaseReplicationStreamProtocol.__init__(self, clock)  # Old style class

        self.server_name = server_name
        self.streamer = streamer
        self.addr = addr

        self.name = None

        # The streams the client has subscribed to and is up to date with
        self.replication_streams = set()

        # The streams the client is currently subscribing to.
        self.connecting_streams = set()

        # Map from stream name to list of updates to send once we've finished
        # subscribing the client to the stream.
        self.pending_rdata = {}

        global next_conn_id
        self.conn_id = next_conn_id
        next_conn_id += 1

    def connectionMade(self):
        self.send_command(ServerCommand(self.server_name))
        BaseReplicationStreamProtocol.connectionMade(self)
        self.streamer.new_connection(self)

    def on_NAME(self, cmd):
        self.name = cmd.data

    def on_USER_SYNC(self, cmd):
        self.streamer.on_user_sync(self.conn_id, cmd.user_id, cmd.is_syncing)

    def on_REPLICATE(self, cmd):
        stream_name = cmd.stream_name
        token = cmd.token

        if stream_name == "ALL":
            # Subscribe to all streams we're publishing to.
            for stream in self.streamer.streams_by_name.iterkeys():
                self.subscripe_to_stream(stream, token)
        else:
            self.subscripe_to_stream(stream_name, token)

    def on_FEDERATION_ACK(self, cmd):
        self.streamer.federation_ack(cmd.token)

    def on_REMOVE_PUSHER(self, cmd):
        self.streamer.on_remove_pusher(cmd.app_id, cmd.push_key, cmd.user_id)

    def onINVALIDATE_CACHE(self, cmd):
        self.streamer.on_invalidate_cache(cmd.cache_func, cmd.keys)

    @defer.inlineCallbacks
    def subscripe_to_stream(self, stream_name, token):
        """Subscribe the remote to a streams.

        This invloves checking if they've missed anything and sending those
        updates down if they have. During that time new updates for the stream
        are queued and sent once we've sent down any missed updates.
        """
        self.replication_streams.discard(stream_name)
        self.connecting_streams.add(stream_name)

        try:
            # Get missing updates
            updates, current_token = yield self.streamer.get_stream_updates(
                stream_name, token,
            )

            # Send all the missing updates
            for update in updates:
                token, row = update[0], update[1]
                self.send_command(RdataCommand(stream_name, token, row))

            # Now we can send any updates that came in while we were subscribing
            pending_rdata = self.pending_rdata.pop(stream_name, [])
            for token, update in pending_rdata:
                self.send_command(RdataCommand(stream_name, token, update))

            # We send a POSITION command to ensure that they have an up to
            # date token (especially useful if we didn't send any updates
            # above)
            self.send_command(PositionCommand(stream_name, current_token))

            # They're now fully subscribed
            self.replication_streams.add(stream_name)
        except Exception as e:
            logger.exception("Failed to handle REPLICATE command")
            self.send_error("failed to handle replicate: %r", e)
        finally:
            self.connecting_streams.discard(stream_name)

    def stream_update(self, stream_name, token, data):
        """Called when a new update is available to stream to clients.

        We need to check if the client is interested in the stream or not
        """
        if stream_name in self.replication_streams:
            # The client is subscribed to the stream
            self.send_command(RdataCommand(stream_name, token, data))
        elif stream_name in self.connecting_streams:
            # The client is being subscribed to the stream
            logger.info("Queuing RDATA %r %r", stream_name, token)
            self.pending_rdata.setdefault(stream_name, []).append((token, data))
        else:
            # The client isn't subscribed
            logger.debug("Dropping RDATA %r %r", stream_name, token)

    def send_sync(self, data):
        self.send_command(SyncCommand(data))

    def connectionLost(self, reason):
        logger.info("Replication connection lost: %r: %r", self, reason)
        self.streamer.lost_connection(self)

    def __str__(self):
        return "ReplicationConnection<name=%s,addr=%s>" % (self.name, self.addr)


class ClientReplicationStreamProtocol(BaseReplicationStreamProtocol):
    VALID_INBOUND_COMMANDS = VALID_SERVER_COMMANDS
    VALID_OUTBOUND_COMMANDS = VALID_CLIENT_COMMANDS

    def __init__(self, client_name, server_name, clock, handler):
        BaseReplicationStreamProtocol.__init__(self, clock)

        self.client_name = client_name
        self.server_name = server_name
        self.handler = handler

        # Map of stream to batched updates. See RdataCommand for info on how
        # batching works.
        self.pending_batches = {}

    def connectionMade(self):
        self.send_command(NameCommand(self.client_name))
        BaseReplicationStreamProtocol.connectionMade(self)

        # Once we've connected subscribe to the necessary streams
        for stream_name, token in self.handler.get_streams_to_replicate().iteritems():
            self.replicate(stream_name, token)

        # Tell the server if we have any users currently syncing (should only
        # happen on synchrotrons)
        currently_syncing = self.handler.get_currently_syncing_users()
        for user_id in currently_syncing:
            self.send_command(UserSyncCommand(user_id, True))

        # We've now finished connecting to so inform the client handler
        self.handler.update_connection(self)

    def on_SERVER(self, cmd):
        if cmd.data != self.server_name:
            logger.error("Connected to wrong remote: %r", cmd.data)
            self.transport.abortConnection()

    def on_RDATA(self, cmd):
        try:
            row = STREAMS_MAP[cmd.stream_name].ROW_TYPE(*cmd.row)
        except Exception:
            logger.exception("Failed to parse RDATA: %r %r", cmd.stream_name, cmd.row)
            raise

        if cmd.token is None:
            # I.e. this is part of a batch of updates for this stream. Batch
            # until we get an update for the stream with a non None token
            self.pending_batches.setdefault(cmd.stream_name, []).append(row)
        else:
            # Check if this is the last of a batch of updates
            rows = self.pending_batches.pop(cmd.stream_name, [])
            rows.append(row)

            self.handler.on_rdata(cmd.stream_name, cmd.token, rows)

    def on_POSITION(self, cmd):
        self.handler.on_position(cmd.stream_name, cmd.token)

    def on_SYNC(self, cmd):
        self.handler.on_sync(cmd.data)

    def replicate(self, stream_name, token):
        """Send the subscription request to the server
        """
        if stream_name not in STREAMS_MAP:
            raise Exception("Invalid stream name %r" % (stream_name,))

        logger.info("Subscribing to replication stream: %r from %r", stream_name, token)

        self.send_command(ReplicateCommand(stream_name, token))

    def connectionLost(self, reason):
        logger.info("Replication connection lost: %r: %r", self, reason)
        self.handler.update_connection(None)
