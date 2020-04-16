# -*- coding: utf-8 -*-
# Copyright:    (C)  by Balázs Dukai. All rights reserved.
# Begin:        2020-04-16
# Email:        b.dukai@tudelft.nl

"""Testing the output configuration."""

from pathlib import Path
import pytest
import yaml

from tile_processor import output, db


@pytest.fixture(scope="module")
def outdir():
    return Path("tmp").absolute() / Path("3DBAG")


@pytest.fixture(scope="function")
def cfg_out(outdir):
    """Output configuration from the YAML config file"""
    outp = yaml.safe_load(
        f"""
    output:
        dir: {outdir}
        prefix: lod13_
        database:
            dbname: bag3d_db
            host: localhost
            port: 5590
            user: bag3d_tester
            password: bag3d_test
            schema: out_schema
    """
    )
    return outp


def test_diroutput(cfg_out, outdir):
    dout = output.DirOutput(cfg_out["output"]["dir"])
    assert dout.path == outdir


def test_dboutput(cfg_out, bag3d_db):
    dbout = output.DbOutput(
        conn=bag3d_db, schema=cfg_out["output"]["database"]["schema"]
    )
    assert (
        dbout.dsn
        == "PG:dbname=bag3d_db host=localhost port=5590 user=bag3d_tester password=bag3d_test schemas=out_schema"
    )
    assert (
        dbout.with_table("sometable")
        == "PG:dbname=bag3d_db host=localhost port=5590 user=bag3d_tester password=bag3d_test schemas=out_schema tables=sometable"
    )
    # Check if the dsn property works by changing the schema
    dbout.schema = "bla bla"
    assert (
        dbout.dsn
        == "PG:dbname=bag3d_db host=localhost port=5590 user=bag3d_tester password=bag3d_test schemas=bla bla"
    )


def test_output(cfg_out, bag3d_db, outdir):
    dout = output.DirOutput(cfg_out["output"]["dir"])
    dbout = output.DbOutput(
        conn=bag3d_db, schema=cfg_out["output"]["database"]["schema"]
    )
    out = output.Output(dir=dout, db=dbout)
    assert (
        out.db.with_table("sometable")
        == "PG:dbname=bag3d_db host=localhost port=5590 user=bag3d_tester password=bag3d_test schemas=out_schema tables=sometable"
    )
    assert out.dir.join_path("AHN") == outdir / "AHN"
