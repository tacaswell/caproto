import logging

import trio
import caproto.benchmarking
from caproto.trio.client import SharedBroadcaster, Context, logger


async def main(
    pv1="XF:31IDA-OP{Tbl-Ax:X1}Mtr.VAL", pv2="XF:31IDA-OP{Tbl-Ax:X2}Mtr.VAL"
):
    """Simple example which connects to two motorsim PVs (by default).

    It tests reading, writing, and subscriptions.
    """

    async with trio.open_nursery() as nursery:
        # Some user function to call when subscriptions receive data.
        called = []

        def user_callback(command):
            print("Subscription has received data: {}".format(command))
            called.append(True)

        broadcaster = SharedBroadcaster(nursery=nursery, log_level="DEBUG")
        print("Registering with the repeater...")
        await broadcaster.register()
        print("Registered.")

        ctx = Context(broadcaster, nursery=nursery, log_level="DEBUG")
        await ctx.search(pv1)
        await ctx.search(pv2)
        # Send out connection requests without waiting for responses...
        chan1 = await ctx.create_channel(pv1)
        chan2 = await ctx.create_channel(pv2)
        # Set up a function to call when subscriptions are received.
        chan1.register_user_callback(user_callback)
        # ...and then wait for all the responses.
        await chan1.wait_for_connection()
        await chan2.wait_for_connection()
        reading = await chan1.read()
        print("reading:", reading)
        sub_id = await chan1.subscribe()
        await chan2.read()
        await chan1.unsubscribe(sub_id)
        await chan1.write((5,))
        reading = await chan1.read()
        print("reading:", reading)
        await chan1.write((6,))
        reading = await chan1.read()
        print("reading:", reading)
        await chan2.disconnect()
        await chan1.disconnect()
        assert called
        print("Done")
        await broadcaster.disconnect()
        print("Broadcaster disconnected")
        nursery.cancel_scope.cancel()  # TODO


if __name__ == "__main__":
    logger.setLevel("DEBUG")
    caproto.benchmarking.set_logging_level("DEBUG")
    logging.basicConfig()

    trio.run(main)
