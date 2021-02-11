-- flag:table
drop table if exists simples_plan cascade;
create table simples_plan
(
    id                   serial primary key,
    type                 text        not null default 'cron' check ( type in ('cron', 'basic') ),
    cron                 text,
    name                 text        not null unique,
    status               text        not null default 'inactive' check ( status in ('active', 'inactive', 'error')),
    executing            bool        not null default false,
    ord                  int         not null default 50 check ( ord between 0 and 100),
    interval_time        int,
    remaining_times      int,
    total_times          int         not null default 0,
    action_name          text        not null,
    exec_after_start     bool        not null default false,
    serial_exec          bool        not null default false,
    error_times          int         not null default 0,
    allow_error_times    int,
    timeout              int         not null,
    timeout_times        int         not null default 0,
    start_time           timestamptz not null default now(),
    end_time             timestamptz,
    create_time          timestamptz not null default now(),
    create_user          text,
    plan_data            text,
    last_exec_start_time timestamptz not null default now(),
    last_exec_end_time   timestamptz not null default now(),
    next_exec_time       timestamptz not null default now(),
    remark               text
);
comment on table simples_plan is '定时任务计划';
comment on column simples_plan.type is '类型，cron：根据表达式执行，basic：根据时间间隔执行';
comment on column simples_plan.cron is 'cron表达式';
comment on column simples_plan.name is '名称';
comment on column simples_plan.status is '状态，inactive 和 error 状态的任务会被执行器忽略';
comment on column simples_plan.executing is '是否有任务正在执行';
comment on column simples_plan.ord is '优先级，0-100，0为最高，默认50';
comment on column simples_plan.interval_time is '每次执行的间隔时间单位秒，用于basic类型';
comment on column simples_plan.remaining_times is '剩余执行次数';
comment on column simples_plan.action_name is '执行的动作名称，客户端根据该名称查找具体的任务实现';
comment on column simples_plan.exec_after_start is '开始否是否立刻执行一次，为false时basic类型开始后会经过 interval_time 秒后再执行';
comment on column simples_plan.serial_exec is '是否串行执行，type为cron类型时，到了执行时间如果上一个任务没有结束且没有超时则跳过当前任务。basic类型时，默认时间间隔从上一次开始时间算起，serial_exec 为true时时间间隔从上一次结束时间开始计算';
comment on column simples_plan.total_times is '总执行次数';
comment on column simples_plan.timeout is '超时时间，单位秒';
comment on column simples_plan.timeout_times is '超时次数';
comment on column simples_plan.error_times is '错误次数';
comment on column simples_plan.allow_error_times is '允许错误次数，超时次数和错误次数之和超过允许错误次数时 status 变更为error';
comment on column simples_plan.start_time is '开始时间';
comment on column simples_plan.end_time is '结束时间';
comment on column simples_plan.create_time is '创建时间';
comment on column simples_plan.create_user is '创建人，无特殊要求';
comment on column simples_plan.plan_data is '任务数据';
comment on column simples_plan.last_exec_start_time is '上一次任务的开始时间';
comment on column simples_plan.last_exec_end_time is '上一次任务的结束时间';
comment on column simples_plan.next_exec_time is '下一次计划执行时间';

drop table if exists simples_scheduler cascade;
create table simples_scheduler
(
    id                  serial primary key,
    name                text        not null,
    start_time          timestamptz not null default now(),
    status              text        not null default 'active' check ( status in ('active', 'inactive', 'dead') ),
    check_interval      int         not null,
    last_heartbeat_time timestamptz not null default now()
);
comment on table simples_scheduler is '客户端列表';
comment on column simples_scheduler.name is '客户端标识，无特殊要求，尽量保证同一时间status状态为active的客户端的name不一样';
comment on column simples_scheduler.start_time is '启动时间';
comment on column simples_scheduler.status is '执行器状态， optional: active inactive dead';
comment on column simples_scheduler.check_interval is '心跳间隔，单位:秒，轮询时如果 now> (last_heartbeat_time + check_interval*3) 则status状态更新为dead';
comment on column simples_scheduler.last_heartbeat_time is '最后一次心跳时间';

drop table if exists simples_task cascade;
create table simples_task
(
    id           serial primary key,
    plain_id     serial,
    scheduler_id serial,
    start_time   timestamptz not null default now(),
    end_time     timestamptz,
    action_name  text        not null,
    init_data    text,
    status       text        not null default 'start' check ( status in ('start', 'stop', 'error', 'timeout') ),
    timeout_time timestamptz not null,
    error_msg    text
);
comment on table simples_task is '任务列表';
comment on column simples_task.plain_id is '计划id';
comment on column simples_task.scheduler_id is '客户端id';
comment on column simples_task.start_time is '开始时间';
comment on column simples_task.end_time is '结束时间';
comment on column simples_task.action_name is '动作名称，和plain_id关联的数据的action_name一致';
comment on column simples_task.init_data is '初始化数据';
comment on column simples_task.status is '状态';
comment on column simples_task.error_msg is '错误信息';
comment on column simples_task.timeout_time is '超时时间';

-- flag:cron function
--  文件格式說明
--  ┌──分鐘（0 - 59）
--  │ ┌──小時（0 - 23）
--  │ │ ┌──日（1 - 31）
--  │ │ │ ┌─月（1 - 12）
--  │ │ │ │ ┌─星期（0 - 6，表示从周日到周六）
--  │ │ │ │ │
--  *  *  *  *  *
-- 支持的符号： , - / ?(日和星期)
create or replace function simples_f_get_next_execution_time(cron text, start_time timestamptz) returns timestamptz
    language plpgsql
    immutable as
$$
declare
    cron_item      text[] := regexp_split_to_array(cron, '\s+');
--     上一次
    l_month        int    := extract(month from start_time);
    l_day          int    := extract(day from start_time);
    l_hour         int    := extract(hour from start_time);
    l_minute       int    := extract(minute from start_time);
--     可用的
    a_week_day_arr int[];
    a_month_arr    int[];
    a_day_arr      int[];
    a_hour_arr     int[];
    a_minute_arr   int[];
--     新的
--     0-6
    n_week_day     int;
--     1-12
    n_month        int;
--     1-31
    n_day          int;
--     0-23
    n_hour         int;
--     0-59
    n_minute       int;
    n_year         int    := extract(year from start_time);
--     表达式
    expr_dow       text;
    expr_month     text;
    expr_day       text;
    expr_hour      text;
    expr_minute    text;
    n_timestamptz  timestamptz;
--     为true表示忽略dayOfWeek(不判断dayOfWeek）,为false表示忽略day（此时day下界永远为1）
    ignore_dow     bool;
    loop_count     int    := 0;
begin
    if array_length(cron_item, 1) <> 5 then
        raise exception 'invalid cron expression, expression can only 5 item';
    end if;
    if cron is null then
        raise exception 'cron expression cannot be null';
    end if;
    if start_time is null then
        raise exception 'start_time cannot be null';
    end if;
    expr_dow = cron_item[5];
    expr_month = cron_item[4];
    expr_day = cron_item[3];
    expr_hour = cron_item[2];
    expr_minute = cron_item[1];

    if (expr_day = '*' and expr_dow = '*')
        or (expr_day = '*' and expr_dow = '?')
        or (expr_day != '*' and expr_day != '?' and expr_dow = '?') then
        ignore_dow = true;
    elsif (expr_day = '?' and expr_dow != '*' and expr_dow != '?') then
        ignore_dow = false;
    else
        raise exception 'invalid cron expression, day and day_of_week cannot be meaningful at the same time';
    end if;
    <<l1>>
    loop
        loop_count = loop_count + 1;
        if loop_count > 300 then
            raise exception 'cycle-index greater than 300, cannot find next time, exit function';
        end if;
        a_week_day_arr = simples_f_filter_arr_in_range(
                simples_f_parse_cron_sub_expr_and_get_range('day_of_week', expr_dow, null, 0, 6),
                null, null
            );
        -- raise notice 'week %',a_week_day_arr;
        a_month_arr = simples_f_filter_arr_in_range(
                simples_f_parse_cron_sub_expr_and_get_range('month', expr_month, null, 1, 12),
                l_month, null
            );
        -- raise notice 'month %',a_month_arr;
        if array_length(a_month_arr, 1) is null then
            n_year = n_year + 1;
            l_month = 1;
            l_day = 1;
            l_hour = 0;
            l_minute = 0;
            continue;
        end if;
        a_day_arr = simples_f_filter_arr_in_range(
                simples_f_parse_cron_sub_expr_and_get_range('day', expr_day, null, 1, 31),
                l_day,
--             取当前月份
                extract(days FROM date_trunc('month', make_date(n_year, a_month_arr[0], 1)) +
                                  interval '1 month - 1 day')::int
            );
        -- raise notice 'day %',a_day_arr;
        if array_length(a_day_arr, 1) is null then
            l_month = l_month + 1;
            l_day = 1;
            l_hour = 0;
            l_minute = 0;
            continue;
        end if;

        a_hour_arr = simples_f_filter_arr_in_range(
                simples_f_parse_cron_sub_expr_and_get_range('hour', expr_hour, null, 0, 23),
                l_hour, null
            );
        -- raise notice 'hour %',a_hour_arr;
        if array_length(a_hour_arr, 1) is null then
            l_day = l_day + 1;
            l_hour = 0;
            l_minute = 0;
            continue;
        end if;
        a_minute_arr = simples_f_filter_arr_in_range(
                simples_f_parse_cron_sub_expr_and_get_range('minute', expr_minute, null, 0, 59),
                l_minute, null
            );
        -- raise notice 'minute %',a_minute_arr;
        if array_length(a_minute_arr, 1) is null then
            l_hour = l_hour + 1;
            l_minute = 0;
            continue;
        end if;
        n_week_day = a_week_day_arr[1];
        n_month = a_month_arr[1];
        n_day = a_day_arr[1];
        n_hour = a_hour_arr[1];

        foreach n_minute in array a_minute_arr
            loop
                n_timestamptz =
                        make_timestamptz(n_year, n_month, n_day, n_hour, n_minute, 0);

                if not ignore_dow then
                    if array_position(a_week_day_arr,
                                      extract(dow from n_timestamptz)::int) is null then
                        l_day = l_day + 1;
                        l_hour = 0;
                        l_minute = 0;
                        continue l1;
                    end if;
                end if;
                if n_timestamptz > start_time then
                    return n_timestamptz;
                end if;
            end loop;
        l_hour = l_hour + 1;
        l_minute = 0;
    end loop;
end;
$$;

comment on function simples_f_get_next_execution_time(text, timestamptz) is '输入cron表达式和开始时间返回下一次执行时间';


create or replace function simples_f_filter_arr_in_range(source int[], min int, max int) returns int[]
    language plpgsql as
$$
declare
    arr int[];
begin
    if min is not null then
        if max is not null then
            select array(select i from unnest(source) as t(i) where i between min and max) into arr;
        else
            select array(select i from unnest(source) as t(i) where i >= min) into arr;
        end if;
    else
        if max is not null then
            select array(select i from unnest(source) as t(i) where i <= max) into arr;
        else
            arr = source;
        end if;
    end if;
    return arr;
end;
$$;
comment on function simples_f_filter_arr_in_range(int[], int, int) is '根据区间上界和下界过滤int数组，上界和下界为空';


create or replace function simples_f_parse_cron_sub_expr_and_get_range(d_name text, expr text, sub_expr text, lp int, rp int) returns int[]
    language plpgsql
    immutable as
$$
declare
    ia      int[];
--     range/step
    e_type  text;
    r_left  int;
    r_right int;
begin
    if sub_expr is null then
        if position(',' in expr) > 0 then
            select array(select distinct unnest(i)
                         from (
                                  select simples_f_parse_cron_sub_expr_and_get_range(d_name, expr,
                                                                                     x, lp, rp) as i
                                  from unnest(regexp_split_to_array(expr, ',')) as t(x)
                              ) t)
            into ia;
            return ia;
        else
            sub_expr := expr;
        end if;
    end if;
    if regexp_match(sub_expr,
                    '^(\?|\*|\d{1,2}-\d{1,2}|\d{1,2}/\d{1,2}|\d{1,2}(,\d{1,2})?)$') is null then
        raise exception 'invalid % group: %',d_name,sub_expr;
    end if;
    if length(sub_expr) = 1 then
        if sub_expr = '*' then
            return ARRAY(SELECT * FROM generate_series(lp, rp));
        elsif sub_expr = '?' then
            if d_name != 'day' and d_name != 'day_of_week' then
                raise exception 'only day and day_of_week group support ''?''';
            else
                return ARRAY(SELECT * FROM generate_series(lp, rp));
            end if;
        else
            r_left = sub_expr::int;
            if r_left < lp then
                raise exception 'invalid % group, % number must be between % and %',d_name,d_name,lp,rp;
            end if;
            return array [sub_expr::int];
        end if;
    elsif length(sub_expr) = 2 then
        r_left = sub_expr::int;
        if r_left < lp or r_left > rp then
            raise exception 'invalid % group, % number must be between % and %',d_name,d_name,lp,rp;
        end if;
        return array [sub_expr::int];
    end if;
    if position('-' in sub_expr) <> 0 then
        e_type = 'range';
    elsif position('/' in sub_expr) <> 0 then
        e_type = 'step';
    end if;
    if e_type = 'range' then
        r_left = (regexp_split_to_array(sub_expr, '-'))[1]::int;
        r_right = (regexp_split_to_array(sub_expr, '-'))[2]::int;
        if r_left < lp or r_left > rp or r_right < lp or r_right > rp then
            raise exception 'invalid % group, % number must be between % and %',d_name,d_name,lp,rp;
        end if;
        if r_right <= r_left then
            raise exception 'invalid % group, right endpoint must be greater than left endpoint',d_name;
        end if;
        ia = ARRAY(SELECT * FROM generate_series(r_left, r_right));
    elsif e_type = 'step' then
--         起始值
        r_left = (regexp_split_to_array(sub_expr, '/'))[1]::int;
--         步长
        r_right = (regexp_split_to_array(sub_expr, '/'))[2]::int;
        if r_left < lp or r_left > rp then
            raise exception 'invalid % group, start value must be between % and %',d_name,lp,rp;
        end if;
        if r_right < 1 or r_right > rp then
            raise exception 'invalid % group, step size must be between 2 and %',d_name,rp - 1;
        end if;
        ia = array [r_left];
        loop
            r_left = r_left + r_right;
            exit when r_left > rp;
            ia = array_append(ia, r_left);
        end loop;
    end if;
    return ia;
end;
$$;
comment on function simples_f_parse_cron_sub_expr_and_get_range(d_name text, expr text, sub_expr text, lp int, rp int) is '解析cron表达式按空格分组后的子表达式，并根据上界和下界返回可用值数组';

-- flag:trigger
create or replace function simples_plan_trigger_fun_insert() returns trigger
    language plpgsql as
$$
declare
    record_new     simples_plan := new;
    next_exec_time timestamptz;
begin
    if record_new.type = 'cron' then
        next_exec_time := simples_f_get_next_execution_time(record_new.cron, record_new.start_time);
    else
        next_exec_time := record_new.start_time + record_new.interval_time * interval '1 second';
    end if;
    if record_new.exec_after_start then
        next_exec_time := record_new.start_time;
    end if;
    record_new.next_exec_time = next_exec_time;
    record_new.last_exec_start_time = record_new.start_time;
    record_new.last_exec_end_time = record_new.start_time;
    return record_new;
end;
$$;
comment on function simples_plan_trigger_fun_insert() is '';
drop trigger if exists simples_plan_insert_trigger on simples_plan;
create trigger simples_plan_insert_trigger
    before insert
    on simples_plan
    for each row
execute function simples_plan_trigger_fun_insert();


create or replace function simples_plan_trigger_fun_update() returns trigger
    language plpgsql as
$$
declare
    record_new     simples_plan := new;
    record_old     simples_plan := old;
    s_time         timestamptz;
    next_exec_time timestamptz;
begin
    --     更新下一次执行时间
    if record_new.type = 'cron' then
        if record_old.start_time <> record_new.start_time then
            s_time := record_new.start_time;
        elsif record_old.last_exec_start_time <> record_new.last_exec_start_time then
            s_time := record_new.last_exec_start_time;
        end if;
        if record_new.cron <> record_old.cron then
            s_time := record_new.last_exec_start_time;
        end if;
        if s_time is not null then
            next_exec_time := simples_f_get_next_execution_time(record_new.cron, s_time);
        end if;
    elsif record_new.type = 'basic' then
        if record_new.serial_exec then
            if record_new.last_exec_end_time <> record_old.last_exec_end_time then
                s_time := record_new.last_exec_end_time;
            end if;
        else
            if record_new.last_exec_start_time <> record_old.last_exec_start_time then
                s_time := record_new.last_exec_start_time;
            end if;
        end if;
        if record_new.serial_exec <> record_old.serial_exec then
            if record_new.serial_exec then
                s_time := record_new.last_exec_end_time;
            else
                s_time := record_new.last_exec_start_time;
            end if;
        end if;
        if record_new.start_time <> record_old.start_time then
            s_time := record_new.start_time;
        end if;
        if s_time is not null then
            next_exec_time := s_time + record_new.interval_time * interval '1 second';
        end if;
    end if;
    if record_new.exec_after_start and record_new.start_time <> record_old.start_time then
        next_exec_time := record_new.start_time;
    end if;
    if next_exec_time is not null then
        record_new.next_exec_time = next_exec_time;
    end if;

--     重新启用计划时清空错误计数
    if record_new.status = 'active' and record_old.status = 'error' then
        record_new.error_times = 0;
        record_new.timeout_times = 0;
    end if;
--  更新状态
    if record_new.remaining_times is not null and record_new.remaining_times = 0 then
        record_new.status = 'inactive';
    end if;
    if record_new.allow_error_times < (record_new.timeout_times + record_new.error_times) then
        record_new.status = 'error';
    end if;
    --     更新执行计数
--     if record_new.total_times = (record_old.total_times + 1) then
--         record_new.remaining_times :=
--                 record_new.remaining_times + 1;
--     end if;
    return record_new;
end;
$$;
comment on function simples_plan_trigger_fun_update() is '计划更新触发器，修改下次执行时间和计划状态';
drop trigger if exists simples_plan_update_trigger on simples_plan;
create trigger simples_plan_update_trigger
    before update
    on simples_plan
    for each row
execute function simples_plan_trigger_fun_update();


-- flag:function
drop function if exists simples_f_keepalive(c_id integer, c_name text, c_interval integer);
create or replace function simples_f_keepalive(c_id integer, c_name text, c_interval integer)
    returns simples_scheduler
    language plpgsql
as
$$
declare
    client simples_scheduler;
begin
    --     c_id scheduler id
    --     c_name scheduler名称
    --     c_interval scheduler轮询间隔
    --     更新客户端状态或创建客户端数据
    if c_interval is null then
        raise exception 'c_interval cannot be null';
    end if;
    if c_name is null then
        raise exception 'c_name cannot be null';
    end if;

--     更新心跳时间或创建新的记录
    update simples_scheduler
    set last_heartbeat_time = current_timestamp
    where id = c_id
      and status = 'active'
    returning * into client;
    if client is null then
        insert into simples_scheduler (name, status, check_interval)
        values (c_name, 'active', c_interval)
        returning * into client;
    end if;

    return client;
end;
$$;
comment on function simples_f_keepalive(c_id integer, c_name text, c_interval integer) is '登录/保持客户端登录状态';

drop function if exists simples_f_mark_dead_scheduler();
create or replace function simples_f_mark_dead_scheduler()
    returns table
            (
                s_id   int,
                s_name text
            )
    language plpgsql
as
$$
begin
    return query update simples_scheduler
        set status = 'dead'
        where id in (
            select ss.id
            from simples_scheduler ss
            where status = 'active'
              and current_timestamp >
                  last_heartbeat_time + (3 * check_interval * interval '1 second')
                for update skip locked
        )
        returning id,name;
end;
$$;
comment on function simples_f_mark_dead_scheduler() is '标记已未正常停止的调度程序';


drop function if exists simples_f_get_task(c_id int, c_name text, c_interval int, task_size int,
                                           name_prefix text, asc_ord bool);
create or replace function simples_f_get_task(c_id int, c_name text, c_interval int, task_size int,
                                              name_prefix text, asc_ord bool)
    returns
        table
        (
            s_id        int,
            action_name text,
            task_id     int,
            init_data   text
        )
    language plpgsql
as
$$
declare
    scheduler simples_scheduler;
    id_arr    int[];
    sql_str   text;
begin
    -- c_id scheduler id
    -- c_name scheduler 名称
    -- c_interval scheduler 轮询间隔，单位秒
    -- task_size 请求的任务数量
    -- name_prefix 计划名称前缀
    -- asc_ord 是否按照 simples_plan.ord 正序搜索
    if task_size is null or task_size < 0 then
        raise exception 'invalid task_size:%, task_size must be greater than or equal to 0',task_size;
    end if;
    if name_prefix is null then
        name_prefix := '';
    end if;
    if asc_ord is null then
        asc_ord := true;
    end if;
    select * into scheduler from simples_f_keepalive(c_id, c_name, c_interval);

--     刷新计划状态
    update simples_plan
    set status = 'active'
    where id in (
        select id
        from simples_plan sp
        where status = 'inactive'
          and current_timestamp > start_time
          and (remaining_times is null or remaining_times > 0)
    );

    if task_size <> 0 then
        -- 查询任务
        sql_str := E'select array_agg(id)\n'
            'from (select id\n'
            '      from simples_plan sp\n'
            '      where sp.status = ''active''\n'
            '        and sp.name like $1\n'
            '        and (\n'
            '              (sp.type = ''cron'' and $2> sp.next_exec_time and\n'
            '               ((sp.serial_exec and (not executing)) or sp.serial_exec = false))\n'
            '              or (sp.type = ''basic'' and\n'
            '                  (\n'
            '                          (sp.serial_exec and (not sp.executing)\n'
            '                              and ($3 > sp.next_exec_time)\n'
            '                              )\n'
            '                          or\n'
            '                          (not sp.serial_exec and\n'
            '                           $4 > sp.next_exec_time\n'
            '                              )\n'
            '                      )\n'
            '                  )\n'
            '          )\n';
        if asc_ord then
            sql_str := sql_str || E'    order by sp.ord\n';
        else
            sql_str := sql_str || E'    order by sp.ord desc\n';
        end if;
        sql_str := sql_str || E'      limit $5 for update skip locked) t;\n';
        execute sql_str using name_prefix || '%',current_timestamp,current_timestamp,current_timestamp,task_size into id_arr;
        if id_arr is not null then
            return query with updated as (
                update simples_plan
                    set
--                         scheduler_id = scheduler.id,
                        executing = true,
                        last_exec_start_time = current_timestamp,
                        last_exec_end_time = current_timestamp,
                        total_times = total_times + 1,
                        remaining_times = remaining_times - 1
                    where id = any (id_arr)
                    returning id,simples_plan.action_name,plan_data,timeout
                )
                insert into simples_task (plain_id, scheduler_id, action_name, init_data,
                                          timeout_time)
                    select upd.id,
                           scheduler.id,
                           upd.action_name,
                           upd.plan_data,
                           current_timestamp + upd.timeout * interval '1 second'
                    from updated upd
                    returning scheduler.id,simples_task.action_name,simples_task.id,simples_task.init_data;
            return;
        end if;
    end if;
    s_id = scheduler.id;
    return next;
    return;
end ;
$$;

comment on function simples_f_get_task(c_id integer, c_name text, c_interval integer, task_size integer, name_prefix text, asc_ord boolean) is '获取任务';

create or replace function simples_f_mark_task_completed(task_id int) returns void
    language plpgsql as
$$
declare
    task simples_task;
begin
    select * into task from simples_task where id = task_id for update;
    if FOUND then
        if task.status = 'start' then
            update simples_task set status = 'stop', end_time= current_timestamp where id = task_id;
            update simples_plan
            set executing= false,
--                 scheduler_id =null,
                last_exec_end_time = current_timestamp
            where id = task.plain_id
              and last_exec_start_time = task.start_time
--               and scheduler_id = task.scheduler_id
            ;
        elsif task.status = 'timeout' then
            update simples_task set end_time = current_timestamp where id = task_id;
        end if;
    end if;
end;
$$;
comment on function simples_f_mark_task_completed(task_id int) is '标记任务已完成';

create or replace function simples_f_mark_task_error(task_id int, msg text) returns void
    language plpgsql as
$$
declare
    task simples_task;
    plan simples_plan;
begin
    select * into task from simples_task where id = task_id for update;
    if FOUND then
        update simples_task
        set status    = 'error',
            end_time  = current_timestamp,
            error_msg = msg
        where id = task_id;
        if task.status = 'start' then
            select * into plan from simples_plan where id = task.plain_id for update;
--             if plan.scheduler_id = task.scheduler_id then
            if plan.last_exec_start_time = task.start_time then
                update simples_plan
                set executing          = false,
--                     scheduler_id       = null
                    last_exec_end_time = current_timestamp,
                    error_times        = error_times + 1
                where id = task.plain_id;
            else
                update simples_plan
                set error_times = error_times + 1
                where id = task.plain_id;
            end if;
        elsif task.status = 'timeout' then
            update simples_plan
            set error_times = error_times + 1
            where id = task.plain_id;
        end if;
    end if;
end;
$$;
comment on function simples_f_mark_task_error(task_id int,msg text) is '标记任务执行失败';

drop function if exists simples_f_mark_timeout_task();
create or replace function simples_f_mark_timeout_task()
    returns table
            (
                plan_id         int,
                plan_name       text,
                action_name     text,
                scheduler_id    int,
                task_id         int,
                task_start_time timestamptz
            )
    language plpgsql
as
$$
declare
    task simples_task;
    plan simples_plan;
begin
    for task in select *
                from simples_task st
                where status = 'start'
                  and current_timestamp > timeout_time
                    for update skip locked
        loop
            select * into plan from simples_plan where id = task.plain_id for update skip locked;
            if found then
                if plan.last_exec_start_time = task.start_time then
--                     if plan.scheduler_id = task.scheduler_id then
                    update simples_plan
                    set executing     = false,
--                         scheduler_id= null,
--                         last_exec_end_time = current_timestamp,
                        timeout_times = timeout_times + 1
                    where id = task.plain_id;
                else
                    update simples_plan
                    set timeout_times = timeout_times + 1
                    where id = task.plain_id;
                end if;
                update simples_task set status= 'timeout' where id = task.id;
                plan_id = plan.id;
                plan_name = plan.name;
                action_name = task.action_name;
                scheduler_id = task.scheduler_id;
                task_id = task.id;
                task_start_time = task.start_time;
                return next;
            end if;
        end loop;
    return;
end;
$$;
comment on function simples_f_mark_timeout_task() is '标记超时的任务';
