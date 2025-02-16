from lib.service.daemon import DaemonConnectionCfg

ECHO_CONN_CFG = DaemonConnectionCfg(
    mod_name='lib.daemon.echo.entry',
    proc_tag='ECHO_DAEMON',
    timeout=0.1,
)
