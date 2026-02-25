def test_imports():
    import pg_jdbc_lib
    from pg_jdbc_lib import PgJdbcClient, PgJdbcConfig

    assert pg_jdbc_lib.__version__
    assert PgJdbcClient is not None
    assert PgJdbcConfig is not None