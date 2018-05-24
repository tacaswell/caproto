import ast
import datetime
import array

import numpy as np
import pytest

import caproto as ca

from caproto import ChannelType
from .epics_test_utils import run_caget, run_caput

caget_checks = sum(
    (
        [
            (pv, dtype),
            (pv, ca.field_types["status"][dtype]),
            (pv, ca.field_types["time"][dtype]),
            (pv, ca.field_types["control"][dtype]),
            (pv, ca.field_types["graphic"][dtype]),
        ]
        for pv in ("int", "pi", "enum")
        for dtype in ca.native_types
    ),
    [],
)

caget_checks += [
    ("char", ChannelType.CHAR),
    ("char", ChannelType.STS_CHAR),
    ("char", ChannelType.TIME_CHAR),
    ("char", ChannelType.GR_CHAR),
    ("char", ChannelType.CTRL_CHAR),
    ("str", ChannelType.STRING),
    ("str", ChannelType.STS_STRING),
    ("str", ChannelType.TIME_STRING),
    ("str", ChannelType.STSACK_STRING),
    ("str", ChannelType.CLASS_NAME),
]


@pytest.mark.parametrize("pv, dbr_type", caget_checks)
def test_with_caget(
    backends, prefix, pvdb_from_server_example, server, pv, dbr_type
):
    caget_pvdb = {
        prefix + pv_: value for pv_, value in pvdb_from_server_example.items()
    }
    pv = prefix + pv
    ctrl_keys = (
        "upper_disp_limit",
        "lower_alarm_limit",
        "upper_alarm_limit",
        "lower_warning_limit",
        "upper_warning_limit",
        "lower_ctrl_limit",
        "upper_ctrl_limit",
        "precision",
    )

    async def client(*client_args):
        # args are ignored for curio and trio servers.
        print("* client caget test: pv={} dbr_type={}".format(pv, dbr_type))
        print("(client args: %s)".format(client_args))

        db_entry = caget_pvdb[pv]
        # native type as in the ChannelData database
        db_native = ca.native_type(db_entry.data_type)
        # native type of the request
        req_native = ca.native_type(dbr_type)

        data = await run_caget(server.backend, pv, dbr_type=dbr_type)
        print("dbr_type", dbr_type, "data:")
        print(data)

        db_value = db_entry.value

        # convert from string value to enum if requesting int
        if db_native == ChannelType.ENUM and not (
            req_native == ChannelType.STRING
            or dbr_type in (ChannelType.CTRL_ENUM, ChannelType.GR_ENUM)
        ):
            db_value = db_entry.enum_strings.index(db_value)
        if req_native in (ChannelType.INT, ChannelType.LONG, ChannelType.CHAR):
            if db_native == ChannelType.CHAR:
                assert int(data["value"]) == ord(db_value)
            else:
                assert int(data["value"]) == int(db_value)
        elif req_native in (ChannelType.STSACK_STRING,):
            db_string_value = db_entry.alarm.alarm_string
            string_length = len(db_string_value)
            read_value = data["value"][:string_length]
            assert read_value == db_string_value
        elif req_native in (ChannelType.CLASS_NAME,):
            assert data["class_name"] == "caproto"
        elif req_native in (ChannelType.FLOAT, ChannelType.DOUBLE):
            assert float(data["value"]) == float(db_value)
        elif req_native == ChannelType.STRING:
            if db_native == ChannelType.STRING:
                db_string_value = str(db_value[0])
                string_length = len(db_string_value)
                read_value = data["value"][:string_length]
                assert int(data["element_count"]) == 1
                assert read_value == db_string_value
                # due to how we monitor the caget output, we get @@@s where
                # null padding bytes are. so long as we verify element_count
                # above and the set of chars that should match, this assertion
                # should pass
            else:
                assert data["value"] == str(db_value)
        elif req_native == ChannelType.ENUM:
            bad_strings = ["Illegal Value (", "Enum Index Overflow ("]
            for bad_string in bad_strings:
                if data["value"].startswith(bad_string):
                    data["value"] = data["value"][len(bad_string) : -1]

            if db_native == ChannelType.ENUM and (
                dbr_type in (ChannelType.CTRL_ENUM, ChannelType.GR_ENUM)
            ):
                # ctrl enum gets back the full string value
                assert data["value"] == db_value
            else:
                assert int(data["value"]) == int(db_value)
        else:
            raise ValueError("TODO " + str(dbr_type))

        # TODO metadata should be cast to requested type as well!
        same_type = ca.native_type(dbr_type) == db_native

        if (
            dbr_type in ca.control_types
            and same_type
            and dbr_type != ChannelType.CTRL_ENUM
        ):
            for key in ctrl_keys:
                if (
                    key == "precision"
                    and ca.native_type(dbr_type) != ChannelType.DOUBLE
                ):
                    print("skipping", key)
                    continue
                print("checking", key)
                assert float(data[key]) == getattr(db_entry, key), key

        if dbr_type in ca.time_types:
            timestamp = datetime.datetime.fromtimestamp(db_entry.timestamp)
            assert data["timestamp"] == timestamp

        if (
            dbr_type in ca.time_types
            or dbr_type in ca.status_types
            or dbr_type == ChannelType.STSACK_STRING
        ):
            severity = data["severity"]
            if not severity.endswith("_ALARM"):
                severity = "{}_ALARM".format(severity)
            severity = getattr(ca._dbr.AlarmSeverity, severity)
            assert severity == db_entry.severity, key

            status = data["status"]
            status = getattr(ca._dbr.AlarmStatus, status)
            assert status == db_entry.status, key

            if "ackt" in data:
                ack_transient = data["ackt"] == "YES"
                assert (
                    ack_transient == db_entry.alarm.must_acknowledge_transient
                )

            if "acks" in data:
                ack_severity = data["acks"]
                ack_severity = getattr(ca._dbr.AlarmSeverity, ack_severity)
                assert ack_severity == db_entry.alarm.severity_to_acknowledge

    server(pvdb=caget_pvdb, client=client)
    print("done")


caput_checks = [
    ("int", "1", [1]),
    ("pi", "3.18", [3.18]),
    ("enum", "d", ["d"]),
    ("enum2", "cc", ["cc"]),
    ("str", "resolve", ["resolve"]),
    ("char", "51", b"3"),
    ("chararray", "testing", ["testing"]),
    # ('bytearray', 'testing', list(b'testing')),
    ("stra", ["char array"], ["char array"]),
]


@pytest.mark.parametrize("pv, put_value, check_value", caput_checks)
# @pytest.mark.parametrize('async_put', [True, False])
def test_with_caput(
    backends,
    prefix,
    pvdb_from_server_example,
    server,
    pv,
    put_value,
    check_value,
    async_put=True,
):

    caget_pvdb = {
        prefix + pv_: value for pv_, value in pvdb_from_server_example.items()
    }
    pv = prefix + pv

    async def client(*client_args):
        # args are ignored for curio and trio servers.
        print(
            "* client put test: {} put value: {} check value: {}"
            "".format(pv, put_value, check_value)
        )
        print("(client args: %s)".format(client_args))

        db_entry = caget_pvdb[pv]
        db_old = db_entry.value
        data = await run_caput(
            server.backend,
            pv,
            put_value,
            as_string=isinstance(db_entry, (ca.ChannelByte, ca.ChannelChar)),
        )
        db_new = db_entry.value

        if isinstance(db_entry, (ca.ChannelInteger, ca.ChannelDouble)):

            def clean_func(v):
                return [ast.literal_eval(v)]

        elif isinstance(db_entry, (ca.ChannelEnum,)):

            def clean_func(v):
                if " " not in v:
                    return [v]
                return [v.split(" ", 1)[1]]

            # db_new = [db_entry.enum_strings[db_new[0]]]
        elif isinstance(db_entry, ca.ChannelByte):
            if pv.endswith("bytearray"):

                def clean_func(v):
                    return np.frombuffer(v.encode("latin-1"), dtype=np.uint8)

            else:

                def clean_func(v):
                    return chr(int(v)).encode("latin-1")

        elif isinstance(db_entry, (ca.ChannelChar, ca.ChannelString)):
            if pv.endswith("stra"):
                # database holds ['char array'], caput shows [len char array]
                def clean_func(v):
                    return [v.split(" ", 1)[1]]

            else:
                # database holds ['string'], caput doesn't show it
                def clean_func(v):
                    return [v]

        else:
            clean_func = None

        if clean_func is not None:
            for key in ("old", "new"):
                data[key] = clean_func(data[key])
        print("caput data", data)
        print("old from db", db_old)
        print("new from db", db_new)
        print("old from caput", data["old"])
        print("new from caput", data["new"])

        if isinstance(db_new, (array.array, np.ndarray)):
            db_new = db_new.tolist()

        # check value from database compared to value from caput output
        assert db_new == data["new"], "left = database/right = caput output"
        # check value from database compared to value the test expects
        assert db_new == check_value, "left = database/right = test expected"

    server(pvdb=caget_pvdb, client=client)
    print("done")
