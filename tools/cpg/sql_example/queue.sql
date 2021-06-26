create schema cpq;

create sequence cpq.sq;

----------------------------------

create type cpq.parse_input_queue as (
    external_id bigint,
    message_ts bigint,
    source text,
    segment bigint,
    message json
);

create table cpq.settings(
	name text not null primary key,
	value text not null
);

insert into cpq.settings (name, value)
    select 'unique_cluster_name', 'unique_name_for_your_cluster'; -- SET YOUR VALUE

----------------------------------

create table cpq.example_queue(
    id bigint not null primary key default nextval('cpq.sq'),
    dt timestamp with time zone default now(),
    segment bigint not null default 0,
    message json not null
);

create or replace function candles.example_queue_get(_limit int default 1000)
   returns table (
            external_id bigint, 
            message_dt timestamp with time zone,
            source text,
            segment bigint, 
            message json
        ) 
   language plpgsql
  as
$$
declare 
begin
    return query (
        select q.id, q.dt, s.value, q.segment, q.message
            from 
                    cpq.example_queue q
                inner join
                    cpq.settings s
                        on s.name = 'unique_cluster_name'
            order by id
            limit _limit
    );
end;
$$;

create or replace function candles.example_queue_complete(_data json)
   returns void 
   language plpgsql
  as
$$
declare _ids bigint[];
begin
    select array_agg(vl::text::bigint) into _ids
        from json_array_elements(_data) vl;
    
    delete from cpq.example_queue as eq
        where eq.id = any(_ids);
end;
$$;

----------------------------------

create table cpq.example_input_queue(
    id bigint not null primary key default nextval('cpq.sq'),
    dt timestamp with time zone default now(),
    external_id bigint not null,
    message_ts bigint not null,
    source text not null,
    segment bigint not null,
    message json
);

create unique index on cpq.example_input_queue (source, external_id, segment);

create or replace function candles.receive_process(_data cpq.parse_input_queue)
   returns void 
   language plpgsql
  as
$$
begin
    insert into cpq.example_input_queue (
                external_id, message_ts, source, segment, message)
        select
            _data.external_id, _data.message_ts, _data.source, 
            _data.segment, _data.message
        on conflict (source, external_id, segment) do nothing;
    -- DO SOMETHING
end;
$$;

create or replace function candles.receive_example(_data json)
   returns void 
   language plpgsql
  as
$$
begin
    perform andles.receive_process(r)
        from (
                select * 
                    from json_populate_recordset(null::cpq.parse_input_queue, _data)
            ) r;
end;
$$;



