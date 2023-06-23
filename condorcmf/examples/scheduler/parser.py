from pathlib import Path

from condorcmf.scheduler import parser


def main():
    log = """
    000 (1436.000.000) 05/24 17:37:04 Job submitted from host: <10.102.32.11:42012?addrs=10.102.32.11-42012+[--1]-42012>
    ...
    000 (1436.001.000) 05/24 17:37:04 Job submitted from host: <10.102.32.11:42012?addrs=10.102.32.11-42012+[--1]-42012>
    ...
    000 (1436.002.000) 05/24 17:37:04 Job submitted from host: <10.102.32.11:42012?addrs=10.102.32.11-42012+[--1]-42012>
    ...
    000 (1436.003.000) 05/24 17:37:04 Job submitted from host: <10.102.32.11:42012?addrs=10.102.32.11-42012+[--1]-42012>
    ...
    000 (1436.004.000) 05/24 17:37:04 Job submitted from host: <10.102.32.11:42012?addrs=10.102.32.11-42012+[--1]-42012>
    ...
    """

    log_dict = parser.parse_log_file(log)
    print(log_dict)

    log_dict = parser.parse_log_file(Path("./utils_sleep/log/log.log"))
    print(log_dict)


if __name__ == "__main__":
    main()
