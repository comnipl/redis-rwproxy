


<h1 align="center">
  redis-rwproxy
</h1>
<p align="center">A transparent Redis proxy that forwards only read paths to read replicas.</p>

<br />

> [!CAUTION]
> This software is currently under development.  
> **Use in a production environment is not advised.**

<br />

## Usage

```sh
$ redis-rwproxy 0.0.0.0:6379 \
    'redis://username:password@master:6379' \
    'redis://username:password@read-replica:6379' \
    --username <username> --password <password>
```
This command listens on `0.0.0.0:6379` and acts like a Redis server.  
It forwards write operations to the master server (specified by the first argument) and read operations to the replica server (specified by the second argument).
