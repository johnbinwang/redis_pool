{application, redis, [
    {description, "Erlang redis client"},
    {vsn, "1.0"},
    {modules, [
      redis,
      redis_app,
      redis_manager,
      redis_pid_sup,
      redis_pool,
      redis_pool_sup,
      redis_shard,
      redis_subscribe,
      redis_sup,
      redis_uri]},
    {applications, [stdlib, kernel, sasl]},
    {registered, []},
    {mod, {redis_app, []}}
]}.
