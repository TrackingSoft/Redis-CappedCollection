package Redis::CappedCollection;
use 5.010;

# Pragmas
use strict;
use warnings;
use bytes;

our $VERSION = '0.01';

use Exporter qw( import );
our @EXPORT_OK  = qw(
    DEFAULT_SERVER
    DEFAULT_PORT
    NAMESPACE

    ENOERROR
    EMISMATCHARG
    EDATATOOLARGE
    ENETWORK
    EMAXMEMORYLIMIT
    EMAXMEMORYPOLICY
    ECOLLDELETED
    EREDIS
    EDATAIDEXISTS
    EOLDERTHANALLOWED
    );

#-- load the modules -----------------------------------------------------------

# Modules
use Mouse;
use Mouse::Util::TypeConstraints;
use Carp;
use Redis;
use Data::UUID;
use Digest::SHA1    qw( sha1_hex );
use List::Util      qw( min );
use Params::Util    qw( _NONNEGINT _STRING _INSTANCE _NUMBER );

#-- declarations ---------------------------------------------------------------

use constant {
    DEFAULT_SERVER      => 'localhost',
    DEFAULT_PORT        => 6379,

    NAMESPACE           => 'Capped',
    MAX_DATASIZE        => 512*1024*1024,       # A String value can be at max 512 Megabytes in length.

    ENOERROR            => 0,
    EMISMATCHARG        => 1,
    EDATATOOLARGE       => 2,
    ENETWORK            => 3,
    EMAXMEMORYLIMIT     => 4,
    EMAXMEMORYPOLICY    => 5,
    ECOLLDELETED        => 6,
    EREDIS              => 7,
    EDATAIDEXISTS       => 8,
    EOLDERTHANALLOWED   => 9,
    };

my @ERROR = (
    'No error',
    'Mismatch argument',
    'Data is too large',
    'Error in connection to Redis server',
    "Command not allowed when used memory > 'maxmemory'",
    'Data may be removed by maxmemory-policy all*',
    'Collection was removed prior to use',
    'Redis error message',
    'Attempt to add data to an existing ID',
    'Attempt to add data over outdated',
    );

my $uuid = new Data::UUID;

my $_lua_namespace  = "local namespace  = '".NAMESPACE."'";
my $_lua_queue_key  = "local queue_key  = namespace..':queue:'..coll_name";
my $_lua_status_key = "local status_key = namespace..':status:'..coll_name";
my $_lua_info_keys  = "local info_keys  = namespace..':I:'..coll_name";
my $_lua_data_keys  = "local data_keys  = namespace..':D:'..coll_name";
my $_lua_time_keys  = "local time_keys  = namespace..':T:'..coll_name";
my $_lua_info_key   = "local info_key   = info_keys..':'..list_id";
my $_lua_data_key   = "local data_key   = data_keys..':'..list_id";
my $_lua_time_key   = "local time_key   = time_keys..':'..list_id";

my %lua_script_body;
my $_lua_list_type = <<"END_LIST_TYPE";
-- Determine the current type of data storage list and the amount of data in the list
-- 'zset' - data, date, time, metadata in separate structures
-- 'hash' - all in one hash

    -- converting to the comparisons that follow
    big_data_threshold = tonumber( redis.call( 'HGET', status_key, 'big_data_threshold' ), 10 )
    -- amount of data in the list, need to know to sequential analysis at 'hash' mode
    list_items   = 0

    if redis.call( 'EXISTS', data_key ) == 1 then
-- there is a characteristic element for 'zset'
        list_type = 'zset'
    else
        if big_data_threshold == 0 then
-- exactly 'zset'
            list_type = 'zset'
        else
-- exactly 'hash'
            list_type  = 'hash'
            if list_exist ~= 0 then
                -- information structure of the list
                list_items = redis.call( 'HGET', info_key, '0' )
                if list_items == nil then
                    -- information on the list there are no items
                    list_exist = 0
                    list_items = 0
                else
                    -- converting to the comparisons that follow
                    list_items = tonumber( list_items, 10 )
                end
            end
        end
    end
END_LIST_TYPE

my $_lua_cleaning = <<"END_CLEANING";
-- deleting old data to make room for new data
-- storage type is not changed

        local coll_length = redis.call( 'HGET', status_key, 'length' )
        -- 'size_delta' - upcoming increase in the size of data (increasing the size of the new data when updating)
        if coll_length + size_delta > size then
            -- as to what size to make room
            local size_limit    = size - advance_cleanup_bytes

            -- how much data to delete obsolete data
            local coll_items            = tonumber( redis.call( 'HGET', status_key, 'items' ), 10 )
            local remaining_iterations  = advance_cleanup_num
            if remaining_iterations == 0 or remaining_iterations > coll_items then
                remaining_iterations = coll_items
            end
            local items_deleted = 0
            local lists_deleted = 0
            local bytes_deleted = 0

            -- sign that had been removed from the list of where we plan to make changes
            local own_data_removed = 0

-- while there is data in the collection and there is insufficient space
            while remaining_iterations > 0 do
                remaining_iterations = remaining_iterations - 1

-- Remember that the data can be removed from different lists, so associated with the data is estimated at each iteration of the loop

                if coll_length + size_delta <= size_limit then
                    break
                end

                -- identifier of the list with the oldest data
                local vals              = redis.call( 'ZRANGE', queue_key, 0, 0 )

-- continue to work with the excess (requiring removal) data and for them using the prefix 'excess_'
                local excess_list_id    = vals[1]
                -- key data structures in the mode 'zset'
                local excess_info_key   = info_keys..':'..excess_list_id
                local excess_data_key   = data_keys..':'..excess_list_id
                local excess_time_key   = time_keys..':'..excess_list_id

                -- variables that characterize the data
                local excess_data_id
                local excess_data
                local excess_data_len
                local excess_data_time

                -- variables that characterize the type of storage and data mode 'hash'
                local excess_list_type
                local excess_list_items
                local excess_i

-- determine the type of storage for disposal
                -- simply because sure that the data exists (the list identifier is present in the queue collection)
                if redis.call( 'EXISTS', excess_data_key ) == 1 then
                    -- there is a characteristic element for 'zset'
                    excess_list_type = 'zset'
                else
                    excess_list_type = 'hash'
                    -- converting to the comparisons that follow
                    excess_list_items = tonumber( redis.call( 'HGET', excess_info_key, '0' ), 10 )
                end

                -- a sign of exhaustion list data
                local list_was_deleted = 0

-- remove the oldest item
                if excess_list_type == 'hash' then

                    -- looking for the oldest data in the list, starting with the number 1
                    -- initializing search
                    excess_i = 1
                    -- to reduce operation using '1.t' instead of excess_i..'.t'
                    excess_data_time = redis.call( 'HGET', excess_info_key, '1.t' )
                    local excess_times = { excess_data_time }
                    -- search for other data
                    for i = 2, excess_list_items do
                        local new_t = redis.call( 'HGET', excess_info_key, i..'.t' )
                        table.insert( excess_times, new_t )
                        if new_t < excess_data_time then
                            excess_i         = i
                            excess_data_time = new_t
                        end
                    end

                    -- characteristics of the oldest of the list
                    local vals       = redis.call( 'HMGET', excess_info_key, excess_i..'.i', excess_i..'.d' )
                    excess_data_id   = vals[1]
                    excess_data      = vals[2]
                    excess_data_len  = #excess_data

                    -- actually remove the oldest
                    redis.call( 'HDEL', excess_info_key, excess_i..'.i', excess_i..'.d', excess_i..'.t' )
                    if excess_list_id == list_id then
                        -- had been removed from the list of where we plan to make changes
                        own_data_removed = own_data_removed + 1
                    end

                    if excess_list_items > 1 then
                        -- If the list has more data

                        -- reordering data is not required when removing the last element from the list
                        if excess_i ~= excess_list_items then
                            -- if the element was removed from the list is not the last one

                            -- obtain information about the last list item
                            local vals = redis.call( 'HMGET', excess_info_key, excess_list_items..'.i', excess_list_items..'.d', excess_list_items..'.t' )
                            -- transfer the data of the last element in the position of the deleted item (to ensure continuity of item numbers)
                            redis.call( 'HMSET', excess_info_key, excess_i..'.i', vals[1], excess_i..'.d', vals[2], excess_i..'.t', vals[3] )
                            excess_times[ excess_i ] = excess_times[ excess_list_items ]
                            -- remove the last item
                            redis.call( 'HDEL', excess_info_key, excess_list_items..'.i', excess_list_items..'.d', excess_list_items..'.t' )

                        end
                        table.remove( excess_times )

                        -- after removal, the number of list items decreased
                        excess_list_items = excess_list_items - 1
                        redis.call( 'HSET', excess_info_key, 0, excess_list_items )

                        -- looking for this is the oldest in the remaining list
                        local oldest_i = 1
                        local oldest_time = excess_times[ oldest_i ]
                        for i = 2, excess_list_items do
                            local new_t = excess_times[ i ]
                            if new_t < oldest_time then
                                oldest_i    = i
                                oldest_time = new_t
                            end
                        end

                        -- obtain and store information about the oldest list item
                        local oldest_data_id = redis.call( 'HGET', excess_info_key, oldest_i..'.i' )
                        redis.call( 'HMSET',    excess_info_key, 't', oldest_time, 'i', oldest_data_id, 'l', excess_data_time )
                        redis.call( 'ZADD',     queue_key, oldest_time, excess_list_id )

                    else
                        -- If the list does not have data
                        excess_list_items = 0
                        list_was_deleted  = 1
                        lists_deleted     = lists_deleted + 1

                        -- delete the data storage structure list
                        redis.call( 'DEL',      excess_info_key )

                    end

                else
                    -- looking for the oldest data in the list ('zset' mode)
                    excess_data_id   = redis.call( 'HGET', excess_info_key, 'i' )    -- oldest_data_id
                    excess_data      = redis.call( 'HGET', excess_data_key, excess_data_id )
                    excess_data_len  = #excess_data
                    excess_data_time = redis.call( 'ZSCORE', excess_time_key, excess_data_id )

                    -- actually remove the oldest item
                    redis.call( 'HDEL', excess_data_key, excess_data_id )
                    if excess_list_id == list_id then
                        -- had been removed from the list of where we plan to make changes
                        own_data_removed = own_data_removed + 1
                    end

                    if redis.call( 'EXISTS', excess_data_key ) == 1 then
                        -- If the list has more data

                        redis.call( 'ZREM', excess_time_key, excess_data_id )
                        vals = redis.call( 'ZRANGE', excess_time_key, 0, 0, 'WITHSCORES' )
                        local oldest_data_id    = vals[1]
                        local oldest_time       = vals[2]
                        redis.call( 'HMSET',    excess_info_key, 't', oldest_time, 'i', oldest_data_id, 'l', excess_data_time )
                        redis.call( 'ZADD',     queue_key, oldest_time, excess_list_id )
                    else
                        -- If the list does not have data
                        list_was_deleted = 1
                        lists_deleted     = lists_deleted + 1

                        -- delete the data storage structure list
                        redis.call( 'DEL',      excess_info_key, excess_time_key )

                    end
                end
                -- amount of data collection decreased
                items_deleted = items_deleted + 1
                coll_items = coll_items - 1

                if list_was_deleted == 1 then
                    -- remove the name of the list from the queue collection
                    redis.call( 'ZREM',     queue_key, excess_list_id )
                end

                -- consider changing the size of the data collection
                bytes_deleted   = bytes_deleted + excess_data_len
                coll_length     = coll_length - excess_data_len
            end

            if items_deleted ~= 0 then
                -- reduce the number of items in the collection
                redis.call( 'HINCRBY', status_key, 'items', -items_deleted )
            end
            if lists_deleted ~= 0 then
                -- reduce the number of lists stored in a collection
                redis.call( 'HINCRBY',  status_key, 'lists', -lists_deleted )
            end
            if bytes_deleted ~= 0 then
                -- reduce the data on the volume of data
                redis.call( 'HINCRBY', status_key, 'length', -bytes_deleted )
            end

-- because be removed the own list data:
-- refine the existence of a list and number of data
            if own_data_removed ~= 0 then
                if list_type == 'hash' then
                    list_items = list_items - own_data_removed
                    if list_items == 0 then
                       list_exist = 0
                    end
                else
                    list_exist = redis.call( 'EXISTS', info_key )
                end
            end

        end
END_CLEANING

$lua_script_body{insert} = <<"END_INSERT";
-- adding data to a list of collections

local coll_name             = ARGV[1]
-- converting to the comparisons that follow
local size                  = tonumber( ARGV[2], 10 )
local advance_cleanup_bytes = tonumber( ARGV[3], 10 )
local advance_cleanup_num   = tonumber( ARGV[4], 10 )
local big_data_threshold    = tonumber( ARGV[5], 10 )
local list_id               = ARGV[6]
local data_id               = ARGV[7]
local data                  = ARGV[8]
-- converting to the comparisons that follow
local data_time             = tonumber( ARGV[9], 10 )

-- key data storage structures
$_lua_namespace
$_lua_queue_key
$_lua_status_key
$_lua_info_keys
$_lua_data_keys
$_lua_time_keys
$_lua_info_key
$_lua_data_key
$_lua_time_key

-- determine whether there is a list of data and a collection
local status_exist      = redis.call( 'EXISTS', status_key )
local list_exist        = redis.call( 'EXISTS', info_key )

-- initialize the data returned from the script
local error             = 0
local result_data_id    = 0

-- if there is a collection
if status_exist == 1 then

-- Determine the current type of data storage list and the amount of data in the list
    local list_type
    local list_items
    $_lua_list_type

    -- Features added data
    local data_len      = #data
    -- upcoming increase in the size of data of the collection
    local size_delta    = data_len

-- deleting obsolete data, if it is necessary
    if size > 0 then
        $_lua_cleaning
    end

-- form the identifier added data
    if data_id == '' then
        if list_exist ~= 0 then
            result_data_id = redis.call( 'HGET', info_key, 'n' )
    -- else
    -- otherwise, the value of the initialization
        end
    else
        result_data_id = data_id
    end

-- verification of the existence of old data with new data identifier
    if list_type == 'zset' then
        if redis.call( 'HEXISTS', data_key, result_data_id ) == 1 then
            error = 8               -- EDATAIDEXISTS
        end
    else
        for i = 1, list_items do
            if redis.call( 'HGET', info_key, i..'.i' ) == result_data_id then
                error = 8           -- EDATAIDEXISTS
                break
            end
        end
    end

    if error == 0 then

-- Validating the time of new data, if required
        if redis.call( 'HGET', status_key, 'older_allowed' ) ~= '1' then
            if list_exist ~= 0 then
                local last_removed_time = tonumber( redis.call( 'HGET', info_key, 'l' ), 10 )
                if last_removed_time > 0 and data_time <= last_removed_time then
                    error = 9   -- EOLDERTHANALLOWED
                end
            end
        end

-- add data to the list
        if error == 0 then
-- Remember that the list and the collection can be automatically deleted after the "crowding out" old data

-- ready to add data

            -- control for testing the case of the new list
            local oldest_time = 1
            if list_exist == 0 then
                -- create information structure data list, if the list does not exist
                oldest_time = 0
                redis.call( 'HMSET',    info_key, 'n', 0, 'l', 0, 't', oldest_time, 'i', '' )
                -- reflect the establishment of the new list
                redis.call( 'HINCRBY',  status_key, 'lists', 1 )
            end

            -- If the ID data has been generated on the basis of the internal counter list
            if data_id == '' then
                -- prepare the counter for the next generation data identifier
                redis.call( 'HINCRBY', info_key, 'n', 1 )
            end

            -- add the amount of new data to the size of the data collection
            redis.call( 'HINCRBY', status_key, 'length', data_len )

            -- initialize the maximum amount of data and the number of items in the collection (the new list)
            if redis.call( 'HEXISTS', status_key, 'size' ) == 0 then
                redis.call( 'HMSET', status_key, 'size', size, 'items', 0 )
            end

            -- reflect the addition of new data
            redis.call( 'HINCRBY',  status_key, 'items', 1 )

-- actually add data to the list
            if list_type == 'zset' then

                -- amount of data in the list
                local tlen = redis.call( 'ZCARD', time_key )
                local future_len = tlen + 1

                -- convert the list 'zset' to 'hash', if necessary
                if future_len <= big_data_threshold then
                    -- create a counter of the number of data in the list
                    redis.call( 'HSET', info_key, 0, future_len )

                    -- Transfers data from the structure 'zset' to structure 'hash'
                    for i = 1, tlen do
                        local vals = redis.call( 'ZRANGE', time_key, i - 1, i - 1, 'WITHSCORES' )
                        redis.call( 'HMSET', info_key, i..'.i', vals[1], i..'.d', redis.call( 'HGET', data_key, vals[1] ), i..'.t', vals[2] )
                    end
                    -- reflect the addition of data
                    redis.call( 'HMSET', info_key, future_len..'.i', result_data_id, future_len..'.d', data, future_len..'.t', data_time )

                    -- delete the list data structure 'zset'
                    redis.call( 'DEL', data_key, time_key )
                else
                    -- reflect the addition of data
                    redis.call( 'HSET', data_key, result_data_id, data )
                    redis.call( 'ZADD', time_key, data_time, result_data_id )
                end

            else

                -- amount of data in the list above has been increased
                list_items = list_items + 1

                -- convert the list 'hash' to 'zset', if necessary
                if list_items > big_data_threshold then
                    for i = 1, list_items - 1 do
                        -- Transfers data from the structure 'hash' to structure 'zset'
                        local vals = redis.call( 'HMGET', info_key, i..'.i', i..'.d', i..'.t' )
                        redis.call( 'ZADD', time_key, vals[3], vals[1] )
                        redis.call( 'HSET', data_key, vals[1], vals[2] )

                        -- delete the record of the 'hash' in the list
                        redis.call( 'HDEL', info_key, i..'.i', i..'.d', i..'.t' )
                    end
                    -- reflect the addition of data
                    redis.call( 'ZADD', time_key, data_time, result_data_id )
                    redis.call( 'HSET', data_key, result_data_id, data )

                    -- remove the count of the number of data
                    redis.call( 'HDEL', info_key, 0 )
                else
                    -- reflect the addition of data
                    redis.call( 'HMSET', info_key, 0, list_items, list_items..'.i', result_data_id, list_items..'.d', data, list_items..'.t', data_time )
                end
            end

-- correct the data on the oldest list data if the oldest data added
            if oldest_time ~= 0 then
                -- if a list existed, then the oldest of this could change
                oldest_time = tonumber( redis.call( 'HGET', info_key, 't' ), 10 )
            end
            if data_time < oldest_time or oldest_time == 0 then
                redis.call( 'HMSET', info_key, 't', data_time, 'i', result_data_id )
                redis.call( 'ZADD', queue_key, data_time, list_id )
            end

        end
    end
end

return { error, status_exist, result_data_id }
END_INSERT

$lua_script_body{update} = <<"END_UPDATE";
-- update the data in the list of collections

local coll_name             = ARGV[1]
-- converting to the comparisons that follow
local size                  = tonumber( ARGV[2], 10 )
local advance_cleanup_bytes = tonumber( ARGV[3], 10 )
local advance_cleanup_num   = tonumber( ARGV[4], 10 )
local big_data_threshold    = tonumber( ARGV[5], 10 )
local list_id               = ARGV[6]
local data_id               = ARGV[7]
local data                  = ARGV[8]

-- key data storage structures
$_lua_namespace
$_lua_queue_key
$_lua_status_key
$_lua_info_keys
$_lua_data_keys
$_lua_time_keys
$_lua_info_key
$_lua_data_key
$_lua_time_key

-- determine whether there is a list of data and a collection
local status_exist  = redis.call( 'EXISTS', status_key )
local list_exist    = redis.call( 'EXISTS', info_key )

-- initialize the data returned from the script
local ret           = 0

-- if there is a collection
if status_exist == 1 then

-- Determine the current type of data storage list and the amount of data in the list
    local list_type
    local list_items
    $_lua_list_type

    -- properties of the data that will be replaced
    local data_exist
    local data_i
    if list_type == 'zset' then
        data_exist = redis.call( 'HEXISTS', data_key, data_id )
    else
        data_exist = 0
        for i = 1, list_items do
            if redis.call( 'HGET', info_key, i..'.i' ) == data_id then
                data_exist = 1
                data_i     = i
                break
            end
        end
    end

-- Change the data, if it exists
    if data_exist == 1 then

-- ready to change data

        -- amount of new data
        local data_len = #data

        -- determine the size of the data that will be replaced
        local cur_data
        if list_type == 'zset' then
            cur_data = redis.call( 'HGET', data_key, data_id )
        else
            cur_data = redis.call( 'HGET', info_key, data_i..'.d' )
        end
        local cur_data_len = #cur_data

        -- upcoming increase in the size of data of the collection (increasing the size of the new data when updating)
        local size_delta   = data_len - cur_data_len

-- deleting obsolete data, if it can be necessary
        if size > 0 then
            $_lua_cleaning
        end

-- data change
-- Remember that the list and the collection can be automatically deleted after the "crowding out" old data
        if list_exist ~= 0 then
            if redis.call( 'HEXISTS', data_key, data_id ) == 1 then
                -- data to be changed were not removed

                -- actually change
                ret = redis.call( 'HSET', data_key, data_id, data )
                -- Return value:
                --      1 if field is a new field in the hash and value was set.
                --      0 if field already exists in the hash and the value was updated.

                -- amount of data in the list
                local tlen = redis.call( 'ZCARD', time_key )
                -- when the data changes, the list can only shorten and the list can be changed only from 'zset' to 'hash'
                -- convert the list 'zset' to 'hash', if necessary
                if tlen <= big_data_threshold then
                    -- create a counter of the number of data in the list
                    redis.call( 'HSET', info_key, 0, tlen )

                    -- Transfers data from the structure 'zset' to structure 'hash'
                    for i = 1, tlen do
                        local vals = redis.call( 'ZRANGE', time_key, i - 1, i - 1, 'WITHSCORES' )
                        redis.call( 'HMSET', info_key, i..'.i', vals[1], i..'.d', redis.call( 'HGET', data_key, vals[1] ), i..'.t', vals[2] )
                    end

                    -- delete the list data structure 'zset'
                    redis.call( 'DEL', data_key, time_key )
                end

                if ret == 0 then
                    -- add the increase in the size of data to the size of the data collection
                    redis.call( 'HINCRBY', status_key, 'length', size_delta )
                    ret = 1
                else
                    -- sort of a mistake
                    ret = 0
                end

            else
                -- if exchanged data is not removed, they should be in the list of 'hash'
                for i = 1, list_items do
                    if redis.call( 'HGET', info_key, i..'.i' ) == data_id then
                        -- actually change
                        if redis.call( 'HSET', info_key, i..'.d', data ) == 0 then
                            -- Return value:
                            --      1 if field is a new field in the hash and value was set.
                            --      0 if field already exists in the hash and the value was updated.

                            -- add the increase in the size of data to the size of the data collection
                            redis.call( 'HINCRBY', status_key, 'length', size_delta )
                            ret = 1
                        else
                            -- sort of a mistake
                            ret = 0
                        end
                        break
                    end
                end
            end
        end

    end
end

return { status_exist, ret }
END_UPDATE

$lua_script_body{receive} = <<"END_RECEIVE";
-- returns the data from the list

local coll_name             = ARGV[1]
-- converting to the comparisons that follow
local big_data_threshold    = tonumber( ARGV[2], 10 )
local list_id               = ARGV[3]
local mode                  = ARGV[4]
local data_id               = ARGV[5]

-- key data storage structures
$_lua_namespace
$_lua_status_key
$_lua_info_keys
$_lua_data_keys
$_lua_info_key
$_lua_data_key

-- determine whether there is a list of data and a collection
local status_exist  = redis.call( 'EXISTS', status_key )
local list_exist    = redis.call( 'EXISTS', info_key )

-- if there is a collection
if status_exist == 1 then

-- Determine the current type of data storage list and the amount of data in the list
    local list_type
    local list_items
    $_lua_list_type

    if mode == 'val' then
-- returns the specified element of the data list

        if list_type == 'zset' then
            return redis.call( 'HGET', data_key, data_id )
        else
            for i = 1, list_items do
                if redis.call( 'HGET', info_key, i..'.i' ) == data_id then
                    return redis.call( 'HGET', info_key, i..'.d' )
                end
            end
            return nil
        end

    elseif mode == 'len' then
-- returns the length of the data list

        if list_type == 'zset' then
            return redis.call( 'HLEN', data_key )
        else
            return redis.call( 'HGET', info_key, '0' )
        end

    elseif mode == 'vals' then
-- returns all the data from the list

        if list_type == 'zset' then
            return redis.call( 'HVALS', data_key )
        else
            local ret = {}
            for i = 1, list_items do
                table.insert( ret, redis.call( 'HGET', info_key, i..'.d' ) )
            end
            return ret
        end

    elseif mode == 'all' then
-- returns all data IDs and data values of the data list

        if list_type == 'zset' then
            return redis.call( 'HGETALL', data_key )
        else
            local ret = {}
            for i = 1, list_items do
                vals = redis.call( 'HMGET', info_key, i..'.i', info_key, i..'.d' )
                table.insert( ret, vals[1] )
                table.insert( ret, vals[2] )
            end
            return ret
        end

    else
        -- sort of a mistake
        return nil
    end
end

return { status_exist, false, false, false }
END_RECEIVE

$lua_script_body{pop_oldest} = <<"END_POP_OLDEST";
-- retrieve the oldest data stored in the collection

local coll_name             = ARGV[1]
-- converting to the comparisons that follow
local big_data_threshold    = tonumber( ARGV[2], 10 )

-- key data storage structures
$_lua_namespace
$_lua_queue_key
$_lua_status_key

-- determine whether there is a list of data and a collection
local status_exist  = redis.call( 'EXISTS', status_key )
local queue_exist   = redis.call( 'EXISTS', queue_key )

-- initialize the data returned from the script
local list_exist    = 0
local list_id       = false
local data          = false

-- if the collection exists and contains data
if queue_exist == 1 and status_exist == 1 then

    -- identifier of the list with the oldest data
    list_id = redis.call( 'ZRANGE', queue_key, 0, 0 )[1]

    -- key data storage structures
    $_lua_info_keys
    $_lua_data_keys
    $_lua_time_keys
    $_lua_info_key
    $_lua_data_key
    $_lua_time_key

    -- determine whether there is a list of data and a collection
    list_exist = redis.call( 'EXISTS', info_key )

-- Determine the current type of data storage list and the amount of data in the list
    local list_type
    local list_items
    $_lua_list_type

    -- Features the oldest data
    local data_id = redis.call( 'HGET', info_key, 'i' )
    local data_time
    local data_i

    -- a sign of exhaustion list data
    local list_was_deleted = 0

-- get data
    if list_type == 'zset' then

        -- actually get data
        data = redis.call( 'HGET', data_key, data_id )
        data_time = redis.call( 'ZSCORE', time_key, data_id )

        -- delete the data from the list
        redis.call( 'HDEL', data_key, data_id )
        -- remember the time of remote data
        redis.call( 'HSET', info_key, 'l', data_time )

        if redis.call( 'EXISTS', data_key ) == 1 then
            -- If the list has more data

            -- delete the information about the time of the data
            redis.call( 'ZREM', time_key, data_id )

            -- obtain information about the data that has become the oldest
            local vals = redis.call( 'ZRANGE', time_key, 0, 0, 'WITHSCORES' )
            local oldest_data_id    = vals[1]
            local oldest_time       = vals[2]

            -- stores information about the data that has become the oldest
            redis.call( 'HMSET',    info_key, 't', oldest_time, 'i', oldest_data_id, 'l', data_time )
            redis.call( 'ZADD',     queue_key, oldest_time, list_id )

            -- amount of data in the list
            local tlen = redis.call( 'ZCARD', time_key )
            -- when the data poped, the list can only shorten and the list can be changed only from 'zset' to 'hash'
            -- convert the list 'zset' to 'hash', if necessary
            if tlen <= big_data_threshold then
                -- create a counter of the number of data in the list
                redis.call( 'HSET', info_key, 0, tlen )

                -- Transfers data from the structure 'zset' to structure 'hash'
                for i = 1, tlen do
                    local vals = redis.call( 'ZRANGE', time_key, i - 1, i - 1, 'WITHSCORES' )
                    redis.call( 'HMSET', info_key, i..'.i', vals[1], i..'.d', redis.call( 'HGET', data_key, vals[1] ), i..'.t', vals[2] )
                end

                -- delete the list data structure 'zset'
                redis.call( 'DEL', data_key, time_key )
            end

        else
            -- if the list is no more data
            list_was_deleted  = 1

            -- delete the list data structure 'zset'
            redis.call( 'DEL',      info_key, time_key )

        end

    else
        -- looking for the oldest data in the list ('hash' mode)

        -- get data
        for i = 1, list_items do
            if redis.call( 'HGET', info_key, i..'.i' ) == data_id then
                local vals = redis.call( 'HMGET', info_key, i..'.d', i..'.t' )
                data       = vals[1]
                data_time  = vals[2]
                data_i     = i
                -- delete the data from the list
                redis.call( 'HDEL', info_key, i..'.i', i..'.d', i..'.t' )
                break
            end
        end

        if list_items > 1 then
            -- If the list has more data

            -- reordering data is not required when removing the last data element
            if data_i ~= list_items then
                -- if the element was removed from the list is not the last one

                -- obtain information about the last list item
                local vals = redis.call( 'HMGET', info_key, list_items..'.i', list_items..'.d', list_items..'.t' )
                -- transfer the data of the last element in the position of the deleted item (to ensure continuity of item numbers)
                redis.call( 'HMSET', info_key, data_i..'.i', vals[1], data_i..'.d', vals[2], data_i..'.t', vals[3] )
                -- remove the last item
                redis.call( 'HDEL', info_key, list_items..'.i', list_items..'.d', list_items..'.t' )

            end

            -- after removal, the number of list items decreased
            list_items = list_items - 1
            redis.call( 'HSET', info_key, 0, list_items )

            -- looking for the oldest data item in the remaining list
            local oldest_i = 1
            local oldest_time = redis.call( 'HGET', info_key, '1.t' )
            for i = 2, list_items do
                local new_t = redis.call( 'HGET', info_key, i..'.t' )
                if new_t < oldest_time then
                    oldest_i    = i
                    oldest_time = new_t
                end
            end

            -- obtain and store information about the oldest data item in the list
            local oldest_data_id = redis.call( 'HGET', info_key, oldest_i..'.i' )
            redis.call( 'HMSET',    info_key, 't', oldest_time, 'i', oldest_data_id, 'l', data_time )
            redis.call( 'ZADD',     queue_key, oldest_time, list_id )

        else
            -- if the list is no more data
            list_items        = 0
            list_was_deleted  = 1

            -- delete the data storage structure list
            redis.call( 'DEL',      info_key )

        end
    end

    if list_was_deleted == 1 then
        -- reduce the number of lists stored in a collection
        redis.call( 'HINCRBY',  status_key, 'lists', -1 )
        -- remove the name of the list from the queue collection
        redis.call( 'ZREM',     queue_key, list_id )
    end

-- reduce the data on the volume of data and the number of items in the collection
    redis.call( 'HINCRBY', status_key, 'length', -#data )
    redis.call( 'HINCRBY', status_key, 'items', -1 )

end

return { queue_exist, status_exist, list_exist, list_id, data }
END_POP_OLDEST

$lua_script_body{collection_info} = <<"END_COLLECTION_INFO";
-- to obtain information on the status of the collection

local coll_name     = ARGV[1]

-- key data storage structures
$_lua_namespace
$_lua_queue_key
$_lua_status_key

-- determine whether there is a collection
local status_exist  = redis.call( 'EXISTS', status_key )

-- if there is a collection
if status_exist == 1 then
    local vals      = redis.call( 'HMGET', status_key, 'length', 'lists', 'items' )
    local oldest    = redis.call( 'ZRANGE', queue_key, 0, 0, 'WITHSCORES' )
    local oldest_time = oldest[2]
    return { status_exist, vals[1], vals[2], vals[3], oldest_time }
end

return { status_exist, false, false, false, false }
END_COLLECTION_INFO

$lua_script_body{info} = <<"END_INFO";
-- to obtain information on the status of the data list

local coll_name             = ARGV[1]
-- converting to the comparisons that follow
local big_data_threshold    = tonumber( ARGV[2], 10 )
local list_id               = ARGV[3]

-- key data storage structures
$_lua_namespace
$_lua_status_key
$_lua_info_keys
$_lua_info_key

-- determine whether there is a list of data and a collection
local status_exist  = redis.call( 'EXISTS', status_key )
local list_exist    = redis.call( 'EXISTS', info_key )

-- initialize the data returned from the script
local items             = false
local oldest_time       = false
local last_removed_time = false

-- if there is a collection
if status_exist == 1 and list_exist == 1 then

    -- key data storage structures
    $_lua_data_keys
    $_lua_data_key

-- Determine the current type of data storage list and the amount of data in the list
    local list_type
    local list_items
    $_lua_list_type

    -- the length of the data list
    if list_type == 'zset' then
        items = redis.call( 'HLEN', data_key )
    else
        items = redis.call( 'HGET', info_key, '0' )
    end

    oldest_time       = redis.call( 'HGET', info_key, 't' )
    last_removed_time = redis.call( 'HGET', info_key, 'l' )

end

return { status_exist, items, oldest_time, last_removed_time }
END_INFO

$lua_script_body{drop_collection} = <<"END_DROP_COLLECTION";
-- to remove the entire collection

local coll_name     = ARGV[1]

-- key data storage structures
$_lua_namespace
$_lua_queue_key
$_lua_status_key
$_lua_info_keys
$_lua_data_keys
$_lua_time_keys

-- initialize the data returned from the script
local ret           = 0     -- the number of deleted items

-- remove the control structures of the collection
if redis.call( 'EXISTS', status_key ) == 1 then
    ret = ret + redis.call( 'DEL', queue_key, status_key )
end

-- each element of the list are deleted separately, as total number of items may be too large to send commands 'DEL'

local arr = redis.call( 'KEYS', info_keys..':*' )
if #arr > 0 then

-- delete information hashes of lists
    for i = 1, #arr do
        ret = ret + redis.call( 'DEL', arr[i] )
    end

-- remove structures store data lists
    local patterns = { data_keys, time_keys }
    for k = 1, #patterns do
        arr = redis.call( 'KEYS', patterns[k]..':*' )
        for i = 1, #arr do
            ret = ret + redis.call( 'DEL', arr[i] )
        end
    end

end

return ret
END_DROP_COLLECTION

$lua_script_body{drop} = <<"END_DROP";
-- to remove the data_list

local coll_name     = ARGV[1]
-- converting to the comparisons that follow
local big_data_threshold    = tonumber( ARGV[2], 10 )
local list_id               = ARGV[3]

-- key data storage structures
$_lua_namespace
$_lua_queue_key
$_lua_status_key
$_lua_info_keys
$_lua_info_key

-- determine whether there is a list of data and a collection
local status_exist  = redis.call( 'EXISTS', status_key )
local list_exist    = redis.call( 'EXISTS', info_key )

-- initialize the data returned from the script
local ret           = 0     -- the number of deleted items

-- if there is a collection
if status_exist == 1 and list_exist == 1 then

    -- key data storage structures
    $_lua_data_keys
    $_lua_time_keys
    $_lua_data_key
    $_lua_time_key

-- Determine the current type of data storage list and the amount of data in the list
    local list_type
    local list_items
    $_lua_list_type

-- determine the size of the data in the list and delete the list structure
    local bytes_deleted = 0
    if list_type == 'zset' then
        local vals = redis.call( 'HVALS', data_key )
        list_items = #vals
        for i = 1, list_items do
            bytes_deleted   = bytes_deleted + #vals[ i ]
        end
        redis.call( 'DEL', info_key, data_key, time_key )
    else
        local data
        for i = 1, list_items do
            data            = redis.call( 'HGET', info_key, i..'.d' )
            bytes_deleted   = bytes_deleted + #data
        end
        redis.call( 'DEL', info_key )
    end

    -- reduce the data on the volume of data
    redis.call( 'HINCRBY',  status_key, 'length', -bytes_deleted )
    -- reduce the number of items in the collection
    redis.call( 'HINCRBY',  status_key, 'items', -list_items )
    -- reduce the number of lists stored in a collection
    redis.call( 'HINCRBY',  status_key, 'lists', -1 )
    -- remove the name of the list from the queue collection
    redis.call( 'ZREM',     queue_key, list_id )

    -- list is removed successfully
    ret = 1

end

return { status_exist, ret }
END_DROP

$lua_script_body{verify_collection} = <<"END_VERIFY_COLLECTION";
-- creation of the collection and characterization of the collection by accessing existing collection

local coll_name          = ARGV[1]
local size               = ARGV[2]
local older_allowed      = ARGV[3]
local big_data_threshold = ARGV[4]

-- key data storage structures
$_lua_namespace
$_lua_status_key

-- determine whether there is a collection
local status_exist  = redis.call( 'EXISTS', status_key )

if status_exist == 1 then
-- if there is a collection

    local vals = redis.call( 'HMGET', status_key, 'size', 'older_allowed', 'big_data_threshold' )
    size                = vals[1]
    older_allowed       = vals[2]
    big_data_threshold  = vals[3]

else
-- if you want to create a new collection

    redis.call( 'HMSET', status_key, 'size', size, 'length', 0, 'lists', 0, 'items', 0, 'older_allowed', older_allowed, 'big_data_threshold', big_data_threshold )

end

return { status_exist, size, older_allowed, big_data_threshold }
END_VERIFY_COLLECTION

subtype __PACKAGE__.'::NonNegInt',
    as 'Int',
    where { $_ >= 0 },
    message { ( $_ || '' ).' is not a non-negative integer!' }
    ;

subtype __PACKAGE__.'::NonEmptNameStr',
    as 'Str',
    where { $_ ne '' and $_ !~ /:/ },
    message { ( $_ || '' ).' is not a non-empty string!' }
    ;

subtype __PACKAGE__.'::DataStr',
    as 'Str',
    where { bytes::length( $_ ) <= MAX_DATASIZE },
    message { "'".( $_ || '' )."' is not a valid data string!" }
    ;

#-- constructor ----------------------------------------------------------------

around BUILDARGS => sub {
    my $orig  = shift;
    my $class = shift;

    if ( _INSTANCE( $_[0], 'Redis' ) )
    {
        my $redis = shift;
        return $class->$orig(
# have to look into the Redis object ...
            redis   => $redis->{server},
            _redis  => $redis,
            @_
            );
    }
    elsif ( _INSTANCE( $_[0], 'Test::RedisServer' ) )
    {
# to test only
        my $redis = shift;
        return $class->$orig(
# have to look into the Test::RedisServer object ...
            redis   => '127.0.0.1:'.$redis->conf->{port},
            @_
            );
    }
    elsif ( _INSTANCE( $_[0], __PACKAGE__ ) )
    {
        my $coll = shift;
        return $class->$orig(
                    redis   => $coll->_server,
                    _redis  => $coll->_redis,
                    @_
                );
    }
    else
    {
        return $class->$orig( @_ );
    }
};

sub BUILD {
    my $self = shift;

    $self->_redis( $self->_redis_constructor )
        unless ( $self->_redis );

    my ( $major, $minor ) = $self->_redis->info->{redis_version} =~ /^(\d+)\.(\d+)/;
    if ( $major < 2 or ( $major == 2 and $minor <= 4 ) )
    {
        $self->_set_last_errorcode( EREDIS );
        confess "Need a Redis server version 2.6 or higher";
    }

    $self->_maxmemory_policy( ( $self->_call_redis( 'CONFIG', 'GET', 'maxmemory-policy' ) )[1] );
    if ( $self->_maxmemory_policy =~ /^all/ )
    {
        $self->_throw( EMAXMEMORYPOLICY );
    }
    $self->_maxmemory(        ( $self->_call_redis( 'CONFIG', 'GET', 'maxmemory'        ) )[1] );
    $self->max_datasize( min $self->_maxmemory, $self->max_datasize )
        if $self->_maxmemory;

    $self->_queue_key(  NAMESPACE.':queue:'.$self->name );
    $self->_status_key( NAMESPACE.':status:'.$self->name );
    $self->_info_keys(  NAMESPACE.':I:'.$self->name );
    $self->_data_keys(  NAMESPACE.':D:'.$self->name );
    $self->_time_keys(  NAMESPACE.':T:'.$self->name );

    $self->_verify_collection;
}

#-- public attributes ----------------------------------------------------------

has 'name'                  => (
    is          => 'ro',
    clearer     => '_clear_name',
    isa         => __PACKAGE__.'::NonEmptNameStr',
    default     => sub { return $uuid->create_str },
    );

has 'size'                  => (
    reader      => 'size',
    writer      => '_set_size',
    clearer     => '_clear_size',
    isa         => __PACKAGE__.'::NonNegInt',
    default     => 0,
    );

has 'advance_cleanup_bytes' => (
    is          => 'rw',
    isa         => __PACKAGE__.'::NonNegInt',
    default     => 0,
    trigger     => sub {
                        my $self = shift;
                        $self->advance_cleanup_bytes <= $self->size || $self->_throw( EMISMATCHARG, 'advance_cleanup_bytes' );
                    },
    );

has 'advance_cleanup_num'   => (
    is          => 'rw',
    isa         => __PACKAGE__.'::NonNegInt',
    default     => 0,
    );

has 'max_datasize'          => (
    is          => 'rw',
    isa         => __PACKAGE__.'::NonNegInt',
    default     => MAX_DATASIZE,
    trigger     => sub {
                        my $self = shift;
                        if ( $self->_redis )
                        {
                            $self->max_datasize <= ( $self->_maxmemory ? min( $self->_maxmemory, MAX_DATASIZE ) : MAX_DATASIZE )
                                || $self->_throw( EMISMATCHARG, 'max_datasize' );
                        }
                    },
    );

has 'older_allowed'         => (
    is          => 'ro',
    isa         => 'Bool',
    default     => 1,
    );

has 'big_data_threshold'    => (
    is          => 'ro',
    isa         => __PACKAGE__.'::NonNegInt',
    default     => 0,
    );

has 'last_errorcode'        => (
    reader      => 'last_errorcode',
    writer      => '_set_last_errorcode',
    isa         => 'Int',
    default     => 0,
    );

#-- private attributes ---------------------------------------------------------

has '_server'               => (
    is          => 'rw',
    init_arg    => 'redis',
    isa         => 'Str',
    default     => DEFAULT_SERVER.':'.DEFAULT_PORT,
    trigger     => sub {
                        my $self = shift;
                        $self->_server( $self->_server.':'.DEFAULT_PORT )
                            unless $self->_server =~ /:/;
                    },
    );

has '_redis'                => (
    is          => 'rw',
# 'Maybe[Test::RedisServer]' to test only
    isa         => 'Maybe[Redis] | Maybe[Test::RedisServer]',
    );

has '_maxmemory'            => (
    is          => 'rw',
    isa         => __PACKAGE__.'::NonNegInt',
    init_arg    => undef,
    );

foreach my $attr_name ( qw(
    _maxmemory_policy
    _queue_key
    _status_key
    _info_keys
    _data_keys
    _time_keys
    ) )
{
    has $attr_name          => (
        is          => 'rw',
        isa         => 'Str',
        init_arg    => undef,
        );
}

has '_lua_scripts'          => (
    is          => 'ro',
    isa         => 'HashRef[Str]',
    lazy        => 1,
    init_arg    => undef,
    builder     => sub { return {}; },
    );

#-- public methods -------------------------------------------------------------

sub insert {
    my $self        = shift;
    my $data        = shift;
    my $list_id     = shift // $uuid->create_str;
    my $data_id     = shift // '';
    my $data_time   = shift // time;

    $data                                                   // $self->_throw( EMISMATCHARG, 'data' );
    ( defined( _STRING( $data ) ) or $data eq '' )          || $self->_throw( EMISMATCHARG, 'data' );
    _STRING( $list_id )                                     // $self->_throw( EMISMATCHARG, 'list_id' );
    $list_id !~ /:/                                         || $self->_throw( EMISMATCHARG, 'list_id' );
    ( defined( _STRING( $data_id ) ) or $data_id eq '' )    || $self->_throw( EMISMATCHARG, 'data_id' );
    ( defined( _NUMBER( $data_time ) ) and $data_time > 0 ) || $self->_throw( EMISMATCHARG, 'data_time' );

    my $data_len = bytes::length( $data );
    $self->size and ( ( $data_len <= $self->size )          || $self->_throw( EMISMATCHARG, 'data' ) );
    ( $data_len <= $self->max_datasize )                    || $self->_throw( EDATATOOLARGE );

    $self->_set_last_errorcode( ENOERROR );

    my ( $error, $status_exist, $result_data_id ) = $self->_call_redis(
        $self->_lua_script_cmd( 'insert' ),
        0,
        $self->name,
        $self->size,
        $self->advance_cleanup_bytes,
        $self->advance_cleanup_num,
        $self->big_data_threshold,
        $list_id,
        $data_id,
        $data,
        $data_time,
        );

    if ( $error ) { $self->_throw( $error ); }

    unless ( $status_exist )
    {
        $self->_clear_sha1;
        $self->_throw( ECOLLDELETED );
    }

    return wantarray ? ( $list_id, $result_data_id ) : $list_id;
}

sub update {
    my $self        = shift;
    my $list_id     = shift;
    my $data_id     = shift;
    my $data        = shift;

    _STRING( $list_id )                             // $self->_throw( EMISMATCHARG, 'list_id' );
    defined( _STRING( $data_id ) )                  || $self->_throw( EMISMATCHARG, 'data_id' );
    $data                                           // $self->_throw( EMISMATCHARG, 'data' );
    ( defined( _STRING( $data ) ) or $data eq '' )  || $self->_throw( EMISMATCHARG, 'data' );

    my $data_len = bytes::length( $data );
    $self->size and ( ( $data_len <= $self->size )  || $self->_throw( EMISMATCHARG, 'data' ) );
    ( $data_len <= $self->max_datasize )            || $self->_throw( EDATATOOLARGE );

    $self->_set_last_errorcode( ENOERROR );

    my ( $status_exist, $ret ) = $self->_call_redis(
        $self->_lua_script_cmd( 'update' ),
        0,
        $self->name,
        $self->size,
        $self->advance_cleanup_bytes,
        $self->advance_cleanup_num,
        $self->big_data_threshold,
        $list_id,
        $data_id,
        $data,
        );

    unless ( $status_exist )
    {
        $self->_clear_sha1;
        $self->_throw( ECOLLDELETED );
    }

    return $ret;
}

sub receive {
    my $self        = shift;
    my $list_id     = shift;
    my $data_id     = shift;

    _STRING( $list_id ) // $self->_throw( EMISMATCHARG, 'list_id' );

    $self->_set_last_errorcode( ENOERROR );

    if ( defined( $data_id ) and $data_id ne '' )
    {
        _STRING( $data_id ) // $self->_throw( EMISMATCHARG, 'data_id' );
        return $self->_call_redis(
            $self->_lua_script_cmd( 'receive' ),
            0,
            $self->name,
            $self->big_data_threshold,
            $list_id,
            'val',
            $data_id,
            );
    }
    else
    {
        if ( wantarray )
        {
            return $self->_call_redis(
                $self->_lua_script_cmd( 'receive' ),
                0,
                $self->name,
                $self->big_data_threshold,
                $list_id,
                defined( $data_id ) ? 'all' : 'vals',
                '',
                );
        }
        else
        {
            return $self->_call_redis(
                $self->_lua_script_cmd( 'receive' ),
                0,
                $self->name,
                $self->big_data_threshold,
                $list_id,
                'len',
                '',
                );
        }
    }
}

sub pop_oldest {
    my $self        = shift;

    my @ret;
    $self->_set_last_errorcode( ENOERROR );

    my ( $queue_exist, $status_exist, $excess_list_exist, $excess_id, $excess_data ) =
        $self->_call_redis(
            $self->_lua_script_cmd( 'pop_oldest' ),
            0,
            $self->name,
            $self->big_data_threshold,
            );

    if ( $queue_exist )
    {
        unless ( $status_exist )
        {
            $self->_clear_sha1;
            $self->_throw( ECOLLDELETED );
        }
        unless ( $excess_list_exist )
        {
            $self->_clear_sha1;
            $self->_throw( EMAXMEMORYPOLICY );
        }
        @ret = ( $excess_id, $excess_data );
    }

    return @ret;
}

sub collection_info {
    my $self        = shift;

    $self->_set_last_errorcode( ENOERROR );

    my @ret = $self->_call_redis(
        $self->_lua_script_cmd( 'collection_info' ),
        0,
        $self->name,
        );

    unless ( shift @ret )
    {
        $self->_clear_sha1;
        $self->_throw( ECOLLDELETED );
    }

    return {
        length      => $ret[0],
        lists       => $ret[1],
        items       => $ret[2],
        oldest_time => $ret[3] ? $ret[3] + 0 : $ret[3],
        };
}

sub info {
    my $self        = shift;
    my $list_id     = shift;

    _STRING( $list_id ) // $self->_throw( EMISMATCHARG, 'list_id' );

    $self->_set_last_errorcode( ENOERROR );

    my @ret = $self->_call_redis(
        $self->_lua_script_cmd( 'info' ),
        0,
        $self->name,
        $self->big_data_threshold,
        $list_id,
        );

    unless ( shift @ret )
    {
        $self->_clear_sha1;
        $self->_throw( ECOLLDELETED );
    }

    return {
        items               => $ret[0],
        oldest_time         => $ret[1] ? $ret[1] + 0 : $ret[1],
        last_removed_time   => $ret[2] ? $ret[2] + 0 : $ret[2],
        };
}

sub exists {
    my $self        = shift;
    my $list_id     = shift;

    _STRING( $list_id ) // $self->_throw( EMISMATCHARG, 'list_id' );

    $self->_set_last_errorcode( ENOERROR );

    return $self->_call_redis( 'EXISTS', $self->_data_list_key( $list_id ) );
}

sub lists {
    my $self        = shift;
    my $pattern     = shift // '*';

    _STRING( $pattern ) // $self->_throw( EMISMATCHARG, 'pattern' );

    $self->_set_last_errorcode( ENOERROR );

    return map { ( $_ =~ /:([^:]+)$/ )[0] } $self->_call_redis( 'KEYS', $self->_data_list_key( $pattern ) );
}

sub drop_collection {
    my $self        = shift;

    $self->_set_last_errorcode( ENOERROR );

    my $ret = $self->_call_redis(
        $self->_lua_script_cmd( 'drop_collection' ),
        0,
        $self->name,
        );

    $self->_clear_name;
    $self->_clear_size;
    $self->_clear_sha1;

    return $ret;
}

sub drop {
    my $self        = shift;
    my $list_id     = shift;

    _STRING( $list_id ) // $self->_throw( EMISMATCHARG, 'list_id' );

    $self->_set_last_errorcode( ENOERROR );

    my ( $status_exist, $ret ) = $self->_call_redis(
        $self->_lua_script_cmd( 'drop' ),
        0,
        $self->name,
        $self->big_data_threshold,
        $list_id,
        );

    unless ( $status_exist )
    {
        $self->_clear_sha1;
        $self->_throw( ECOLLDELETED );
    }

    return $ret;
}

sub quit {
    my $self        = shift;

    $self->_set_last_errorcode( ENOERROR );
    $self->_clear_sha1;
    eval { $self->_redis->quit };
    $self->_redis_exception( $@ )
        if $@;
}

#-- private methods ------------------------------------------------------------

sub _lua_script_cmd {
    my $self        = shift;
    my $name        = shift;

    my $sha1 = $self->_lua_scripts->{ $name };
    unless ( $sha1 )
    {
        $sha1 = $self->_lua_scripts->{ $name } = sha1_hex( $lua_script_body{ $name } );
        unless ( ( $self->_call_redis( 'SCRIPT', 'EXISTS', $sha1 ) )[0] )
        {
            return( 'EVAL', $lua_script_body{ $name } );
        }
    }
    return( 'EVALSHA', $sha1 );
}

sub _data_list_key {
    my $self        = shift;
    my $list_id     = shift;

    return( $self->_data_keys.':'.$list_id );
}

sub _verify_collection {
    my $self    = shift;

    $self->_set_last_errorcode( ENOERROR );

    my ( $status_exist, $size, $older_allowed, $big_data_threshold ) = $self->_call_redis(
        $self->_lua_script_cmd( 'verify_collection' ),
        0,
        $self->name,
        $self->size || 0,
        $self->older_allowed ? 1 : 0,
        $self->big_data_threshold || 0,
        );

    if ( $status_exist )
    {
        $self->_set_size( $size ) unless $self->size;
        $size               == $self->size               or $self->_throw( EMISMATCHARG, 'size' );
        $older_allowed      == $self->older_allowed      or $self->_throw( EMISMATCHARG, 'older_allowed' );
        $big_data_threshold == $self->big_data_threshold or $self->_throw( EMISMATCHARG, 'big_data_threshold' );
    }
}

sub _throw {
    my $self    = shift;
    my $err     = shift;
    my $prefix  = shift;

    $self->_set_last_errorcode( $err );
    confess( ( $prefix ? "$prefix : " : '' ).$ERROR[ $err ] );
}

sub _redis_exception {
    my $self    = shift;
    my $error   = shift;

# Use the error messages from Redis.pm
    if (
           $error =~ /^Could not connect to Redis server at /
        or $error =~ /^Can't close socket: /
        or $error =~ /^Not connected to any server/
# Maybe for pub/sub only
        or $error =~ /^Error while reading from Redis server: /
        or $error =~ /^Redis server closed connection/
        )
    {
        $self->_set_last_errorcode( ENETWORK );
    }
    elsif (
           $error =~ /[\S+\s?]ERR command not allowed when used memory > 'maxmemory'/
        or $error =~ /[\S+\s?]OOM command not allowed when used memory > 'maxmemory'/
        or $error =~ /\[EVALSHA\] NOSCRIPT No matching script. Please use EVAL./
        )
    {
        $self->_set_last_errorcode( EMAXMEMORYLIMIT );
        $self->_clear_sha1;
    }
    else
    {
        $self->_set_last_errorcode( EREDIS );
    }

    die $error;
}

sub _clear_sha1 {
    my $self    = shift;

    delete @{$self->_lua_scripts}{ keys %{$self->_lua_scripts} };
}

sub _redis_constructor {
    my $self    = shift;

    $self->_set_last_errorcode( ENOERROR );
    my $redis;
    eval { $redis = Redis->new( server => $self->_server ) };
    $self->_redis_exception( $@ )
        if $@;
    return $redis;
}

# Keep in mind the default 'redis.conf' values:
# Close the connection after a client is idle for N seconds (0 to disable)
#    timeout 300

# Send a request to Redis
sub _call_redis {
    my $self        = shift;
    my $method      = shift;

    my @return;
    $self->_set_last_errorcode( ENOERROR );
    @return = eval { return $self->_redis->$method( map { ref( $_ ) ? $$_ : $_ } @_ ) };
    $self->_redis_exception( $@ )
        if $@;

    return wantarray ? @return : $return[0];
}

#-- Closes and cleans up -------------------------------------------------------

no Mouse::Util::TypeConstraints;
no Mouse;                                       # keywords are removed from the package
__PACKAGE__->meta->make_immutable();

__END__

=head1 NAME

Redis::CappedCollection - Provides fixed sized collections that have
a auto-FIFO age-out feature.

=head1 VERSION

This documentation refers to C<Redis::CappedCollection> version 0.01

=head1 SYNOPSIS

    #-- Common
    use Redis::CappedCollection qw( DEFAULT_SERVER DEFAULT_PORT )

    my $server = DEFAULT_SERVER.':'.DEFAULT_PORT;
    my $coll = Redis::CappedCollection->new( redis => $server );

    #-- Producer
    my $list_id = $coll->insert( 'Some data stuff' );

    # Change the element of the list with the ID $list_id
    $updated = $coll->update( $list_id, $data_id, 'Some new data stuff' );

    #-- Consumer
    # Get data from a list with the ID $list_id
    @data = $coll->receive( $list_id );
    # or to obtain the data in the order they are received
    while ( my ( $list_id, $data ) = $coll->pop_oldest )
    {
        print "List '$list_id' had '$data'\n";
    }

To see a brief but working code example of the C<Redis::CappedCollection>
package usage look at the L</"An Example"> section.

To see a description of the used C<Redis::CappedCollection> data
structure (on Redis server) look at the L</"CappedCollection data structure">
section.

=head1 ABSTRACT

Redis::CappedCollection module provides fixed sized collections that have
a auto-FIFO age-out feature.

=head1 DESCRIPTION

The module provides an object oriented API.
This makes a simple and powerful interface to these services.

The main features of the package are:

=over 3

=item *

Provides an object oriented model of communication.

=item *

Support the work with data structures on the Redis server.

=item *

Supports the automatic creation of capped collection, status monitoring,
updating the data set, obtaining consistent data from the collection,
automatic data removal, the classification of possible errors.

=item *

Simple methods for organizing producer and consumer clients.

=back

Capped collections are fixed sized collections that have an auto-FIFO
age-out feature (age out is based on the time of the corresponding inserted data).
With the built-in FIFO mechanism, you are not at risk of using
excessive disk space.
Capped collections keep data in their time corresponding inserted data order
automatically (in the respective lists of data).
Capped collections automatically maintain insertion order for the data lists
in the collection.

You may insert new data in the capped collection.
If there is a list with the specified ID, the data is inserted into the existing list,
otherwise it is inserted into a new list.

You may update the existing data in the collection.

Once the space is fully utilized, newly added data will replace
the oldest data in the collection.
Limits are specified when the collection is created.

Old data with the same time will be forced out in no specific order.

The collection does not allow deleting data.

Automatic Age Out:
If you know you want data to automatically "roll out" over time as it ages,
a capped collection can be an easier way to support than writing manual removal
via cron scripts.

=head2 CONSTRUCTOR

=head3 C<new( redis =E<gt> $server, ... )>

It generates a C<Redis::CappedCollection> object to communicate with
the Redis server and can be called as either a class method or an object method.
If invoked with no arguments the constructor C<new> creates and returns
a C<Redis::CappedCollection> object that is configured
to work with the default settings.

If invoked with the first argument being an object of C<Redis::CappedCollection>
class or L<Redis|Redis> class, then the new object connection attribute values
are taken from the object of the first argument. It does not create
a new connection to the Redis server.

Since L<Redis|Redis> knows nothing about encoding, it is forcing utf-8 flag
on all data.
If you want to store binary data in the C<Redis::CappedCollection> collection,
you can disable this automatic encoding by passing an option to
L<Redis|Redis> C<new>: C<encoding =E<gt> undef>.

C<new> optionally takes arguments. These arguments are in key-value pairs.

This example illustrates a C<new()> call with all the valid arguments:

    my $coll = Redis::CappedCollection->new(
        redis   => "$server:$port", # Default Redis local server and port
        name    => 'Some name', # If 'name' is not specified, it creates
                        # a new collection named as UUID.
                        # If specified, the work is done with
                        # a given collection or a collection is
                        # created with the specified name.
        size            => 100_000, # The maximum size, in bytes,
                        # of the capped collection data.
                        # Default 0 - no limit.
                        # Is set for the new collection.
        advance_cleanup_bytes => 50_000, # The minimum size, in bytes,
                        # of the data to be released, if the size
                        # of the collection data after adding new data
                        # may exceed 'size'
                        # Default 0 - additional data should not be released.
        advance_cleanup_num => 1_000, # Maximum number of data
                        # elements to delete, if the size
                        # of the collection data after adding new data
                        # may exceed 'size'
                        # Default 0 - the number of times the deleted data
                        # is not limited.
        max_datasize    => 1_000_000,   # Maximum size, in bytes, of the data.
                        # Default 512MB.
        older_allowed   => 0, # Permission to add data with time is less
                        # than the data time of which was deleted from the list.
                        # Default 0 - insert too old data is prohibited.
                        # Is set for the new collection.
        big_data_threshold => 0, # The maximum number of items of data to store
                        # in an optimized format to reduce memory usage.
                        # Default 0 - storage are separate hashes and
                        # ordered lists.
                        # Is set for the new collection.
                        # To see a description of the used
                        # Redis::CappedCollection data structure
                        # (on Redis server) look at the
                        # "CappedCollection data structure" section.
        );

Requirements for arguments C<name>, C<size>, are described in more detail
in the sections relating to the methods L</name>, L</size> .

If the value of C<name> is specified, the work is done with a given collection
or creates a new collection with the specified name.
Do not use the symbol C<':'> in C<name>.

The following examples illustrate other uses of the C<new> method:

    $coll = Redis::CappedCollection->new();
    my $next_coll = Redis::CappedCollection->new( $coll );

    my $redis = Redis->new( redis => "$server:$port" );
    $next_coll = Redis::CappedCollection->new( $redis );

An error will cause the program to halt (C<confess>) if an argument is not valid.

=head2 METHODS

An error will cause the program to halt (C<confess>) if an argument is not valid.

ATTENTION: In the L<Redis|Redis> module the synchronous commands throw an
exception on receipt of an error reply, or return a non-error reply directly.

=head3 C<insert( $data, $list_id, $data_id, $data_time )>

Adds data to the capped collection on the Redis server.

Data obtained in the first argument.
Data should be a string whose length should not exceed the value available
through the method L</max_datasize>.

Data is added to the existing list, if the second argument is specified
and the corresponding queue already exists.
Otherwise, the data is added to a new queue for which ID is the second argument.
ID in the second argument must be a non-empty string (if specified).
Do not use the symbol C<':'> in C<$list_id>.

If the second argument is not specified, the data is added to a new list
with automatically generated ID (UUID).

The data may be given a unique ID and a unique time.
If the ID is not specified (or an empty string), it will be automatically
generated in the form of sequential integer.
Data ID must be unique for a list of data.

Time must be a non-negative number. If time is not specified, it will be
automatically generated as the result of a function
C<time>.

For measuring time in better granularity than one second, use the
L<Time::HiRes|Time::HiRes> module.
In this case, time value is stored on the server as a floating point number
within 4 decimal places.

The following examples illustrate uses of the C<insert> method:

In a scalar context, the method returns the ID of the data list to which
you add the data.

    $list_id = $coll->insert( 'Some data stuff', 'Some_id' );
    # or
    $list_id = $coll->insert( 'Some data stuff' );

In a list context, the method returns the ID of the data list to which
your adding the data and the data ID corresponding to your data.

    ( $list_id, $data_id ) = $coll->insert( 'Some data stuff', 'Some_id' );
    # or
    ( $list_id, $data_id ) = $coll->insert( 'Some data stuff' );

=head3 C<update( $list_id, $data_id, $data )>

Updates the data in the queue identified by the first argument.
C<$list_id> must be a non-empty string.

The updated data ID given as the second argument.
The C<$data_id> must be a non-empty string.

New data should be included in the third argument.
Data should be a string whose length should not exceed the value available
through the method L</max_datasize>.

The following examples illustrate uses of the C<update> method:

    if ( $coll->update( $list_id, 0, 'Some new data stuff' ) )
    {
        print "Data updated successfully\n";
    }
    else
    {
        print "The data is not updated\n";
    }

Method returns true if the data is updated or false if the queue with
the given ID does not exist or is used an invalid data ID.

=head3 C<receive( $list_id, $data_id )>

If the C<$data_id> argument is not specified or is an empty string:

=over 3

=item *

In a list context, the method returns all the data from the list given by
the C<$list_id> identifier.
If the C<$data_id> argument is an empty string then it returns all data IDs and
data values of the data list.
These returns are not ordered.

Method returns an empty list if the list with the given ID does not exist.

=item *

In a scalar context, the method returns the length of the data list given by
the C<$list_id> identifier.

=back

If the C<$data_id> argument is specified:

=over 3

=item *

The method returns the specified element of the data list.
If the data with C<$data_id> ID does not exists, C<undef> is returned.

=back

C<$list_id> must be a non-empty string.
C<$data_id> must be a normal string.

The following examples illustrate uses of the C<receive> method:

    my @data = $coll->receive( $list_id );
    print "List '$list_id' has '$_'\n" foreach @data;
    # or
    my $list_len = $coll->receive( $list_id );
    print "List '$list_id' has '$list_len' item(s)\n";
    # or
    my $data = $coll->receive( $list_id, 0 );
    print "List '$list_id' has '$data' in 'zero' position\n";

=head3 C<pop_oldest>

The method is designed to retrieve the oldest data stored in the collection.

Returns a list of two elements.
The first element contains the identifier of the list from which the data was retrieved.
The second element contains the extracted data.
When retrieving data, it is removed from the collection.

If you perform a C<pop_oldest> on the collection, the data will always
be returned in order of the time corresponding inserted data.

Method returns an empty list if the collection does not contain any data.

The following examples illustrate uses of the C<pop_oldest> method:

    while ( my ( $list_id, $data ) = $coll->pop_oldest )
    {
        print "List '$list_id' had '$data'\n";
    }

=head3 C<collection_info>

The method is designed to obtain information on the status of the collection
and to see how much space an existing collection uses.

Returns a reference to a hash with the following elements:

=over 3

=item *

C<length> - Size in bytes of all the data stored in all the collection lists.

=item *

C<lists> - Number of lists of data stored in a collection.

=item *

C<items> - Number of data items stored in the collection.

=item *

C<oldest_time> - Time corresponding to the oldest data in the collection.
C<undef> if the collection does not contain data.

=back

The following examples illustrate uses of the C<collection_info> method:

    my $info = $coll->collection_info;
    print 'An existing collection uses ', $info->{length}, ' byte of data, ',
        'in ', $info->{items}, ' items are placed in ',
        $info->{lists}, ' lists', "\n";

=head3 C<info( $list_id )>

The method is designed to obtain information on the status of the data list
and to see how many items it contains.

C<$list_id> must be a non-empty string.

Returns a reference to a hash with the following elements:

=over 3

=item *

C<items> - Number of data items stored in the data list.

=item *

C<oldest_time> - Time corresponding to the oldest data in the list.
C<undef> if the data list does not exists.

=item *

C<last_removed_time> - time corresponding to the latest data deleted from the list.
C<undef> if the data list does not exists.

=back

=head3 C<exists( $list_id )>

The method is designed to test whether there is a list in the collection with
ID C<$list_id>.
Returns true if the list exists and false otherwise.

The following examples illustrate uses of the C<exists> method:

    print "The collection has '$list_id' list\n" if $coll->exists( 'Some_id' );

=head3 C<lists( $pattern )>

Returns a list of identifiers stored in a collection.
Returns all list IDs matching C<$pattern> if C<$pattern> is not empty.
C<$patten> must be a non-empty string.

Supported glob-style patterns:

=over 3

=item *

C<h?llo> matches C<hello>, C<hallo> and C<hxllo>

=item *

C<h*llo> matches C<hllo> and C<heeeello>

=item *

C<h[ae]llo> matches C<hello> and C<hallo>, but not C<hillo>

=back

Use C<'\'> to escape special characters if you want to match them verbatim.

The following examples illustrate uses of the C<lists> method:

    print "The collection has '$_' list\n" foreach $coll->lists;

Warning: consider C<lists> as a command that should only be used in production
environments with extreme care. It may ruin performance when it is executed
against large databases.
This command is intended for debugging and special operations.
Don't use C<lists> in your regular application code.

Methods C<lists> can cause an exception (C<confess>) if
the collection contains a very large number of lists
(C<'Error while reading from Redis server'>).

=head3 C<drop_collection>

Use the C<drop_collection> method to remove the entire collection.
Method removes all the structures on the Redis server associated with
the collection.
After using C<drop_collection> you must explicitly recreate the collection.

Before use, make sure that the collection is not being used by other customers.

The following examples illustrate uses of the C<drop_collection> method:

    $coll->drop_collection;

Warning: consider C<drop_collection> as a command that should only be used in production
environments with extreme care. It may ruin performance when it is executed
against large databases.
This command is intended for debugging and special operations.
Don't use C<drop_collection> in your regular application code.

Methods C<drop_collection> can cause an exception (C<confess>) if
the collection contains a very large number of lists
(C<'Error while reading from Redis server'>).

=head3 C<drop( $list_id )>

Use the C<drop> method to remove the entire specified list.
Method removes all the structures on the Redis server associated with
the specified list.

C<$list_id> must be a non-empty string.

Method returns true if the list is removed, or false otherwise.

=head3 C<quit>

Ask the Redis server to close the connection.

The following examples illustrate uses of the C<quit> method:

    $jq->quit;

=head3 C<name>

The method of access to the C<name> attribute (collection ID).
The method returns the current value of the attribute.
The C<name> attribute value is used in the L<constructor|/CONSTRUCTOR>.

If the value of C<name> is not specified to the L<constructor|/CONSTRUCTOR>,
it creates a new collection ID as UUID.

=head3 C<size>

The method of access to the C<size> attribute - the maximum size, in bytes,
of the capped collection data (Default 0 - no limit).

The method returns the current value of the attribute.
The C<size> attribute value is used in the L<constructor|/CONSTRUCTOR>.

Is set for the new collection. Otherwise an error will cause the program to halt
(C<confess>) if the value is not equal to the value that was used when
a collection was created.

=head3 C<advance_cleanup_bytes>

The method of access to the C<advance_cleanup_bytes> attribute - the minimum size,
in bytes, of the data to be released, if the size of the collection data
after adding new data may exceed L</size>. Default 0 - additional data
should not be released.

The C<advance_cleanup_bytes> attribute is designed to reduce the release of memory
operations with frequent data changes.

The C<advance_cleanup_bytes> attribute value can be used in the L<constructor|/CONSTRUCTOR>.
The method returns and sets the current value of the attribute.

The C<advance_cleanup_bytes> value may be less than or equal to L</size>. Otherwise 
an error will cause the program to halt (C<confess>).

=head3 C<advance_cleanup_num>

The method of access to the C<advance_cleanup_num> attribute - maximum number
of data elements to delete, if the size of the collection data after adding
new data may exceed C<size>. Default 0 - the number of times the deleted data
is not limited.

The C<advance_cleanup_num> attribute is designed to reduce the number of
deletes data.

The C<advance_cleanup_num> attribute value can be used in the L<constructor|/CONSTRUCTOR>.
The method returns and sets the current value of the attribute.

=head3 C<max_datasize>

The method of access to the C<max_datasize> attribute.

The method returns the current value of the attribute if called without arguments.

Non-negative integer value can be used to specify a new value to
the maximum size of the data introduced into the collection
(methods L</insert> and L</update>).

The C<max_datasize> attribute value is used in the L<constructor|/CONSTRUCTOR>
and operations data entry on the Redis server.

The L<constructor|/CONSTRUCTOR> uses the smaller of the values of 512MB and
C<maxmemory> limit from a C<redis.conf> file.

=head3 C<older_allowed>

The method of access to the C<older_allowed> attribute -
permission to add data with time less than the data time which was
deleted from the data list. Default 0 - insert too old data is prohibited.

The method returns the current value of the attribute.
The C<older_allowed> attribute value is used in the L<constructor|/CONSTRUCTOR>.

=head3 C<big_data_threshold>

The method of access to the C<big_data_threshold> attribute - determines the maximum
data size to store in an optimzed format.  Data stored in this way will use less memory
but require additional processing to access.
Default 0 - storage are separate hashes and ordered lists.

Can be used to save memory.
It makes sense to use on small lists of data.
Leads to a decrease in performance when using large lists of data.

The method returns the current value of the attribute.
The C<big_data_threshold> attribute value is used in the L<constructor|/CONSTRUCTOR>.

To see a description of the used C<Redis::CappedCollection> data structure
(on Redis server) look at the L</CappedCollection data structure> section.

The effective value of C<big_data_threshold> depends on the conditions the collection:
the item size and length of lists, the size of data identifiers, etc.

The effective value of C<big_data_threshold> can be controlled with server settings
(C<hahs-max-ziplist-entries>, C<hash-max-ziplist-value>,
C<zset-max-ziplist-entries>, C<zset-max-ziplist-value>)
in the C<redis.conf> file.

=head3 C<last_errorcode>

The method of access to the code of the last identified errors.

To see more description of the identified errors look at the L</DIAGNOSTICS>
section.

=head2 EXPORT

None by default.

Additional constants are available for import, which can be used
to define some type of parameters.

These are the defaults:

=over

=item C<DEFAULT_SERVER>

Default Redis local server - C<'localhost'>.

=item C<DEFAULT_PORT>

Default Redis server port - 6379.

=item C<NAMESPACE>

Namespace name used keys on the Redis server - C<'Capped'>.

=back

=head2 DIAGNOSTICS

The method for the possible error to analyse: L</last_errorcode>.

A L<Redis|Redis> error will cause the program to halt (C<confess>).
In addition to errors in the L<Redis|Redis> module, detected errors are
L</EMISMATCHARG>, L</EDATATOOLARGE>, L</EMAXMEMORYPOLICY>, L</ECOLLDELETED>,
L</EDATAIDEXISTS>, L</EOLDERTHANALLOWED>.

All recognizable errors in C<Redis::CappedCollection> lead to
the installation of the corresponding value in the L</last_errorcode> and cause
an exception (C<confess>).
Unidentified errors cause an exception (L</last_errorcode> remains equal to 0).
The initial value of C<$@> is preserved.

The user has the choice:

=over 3

=item *

Use the module methods and independently analyze the situation without the use
of L</last_errorcode>.

=item *

Piece of code wrapped in C<eval {...};> and analyze L</last_errorcode>
(look at the L</"An Example"> section).

=back

In L</last_errortsode> recognizes the following:

=over 3

=item C<ENOERROR>

No error.

=item C<EMISMATCHARG>

This means that you didn't give the right argument to a C<new>
or to other L<method|/METHODS>.

=item C<EDATATOOLARGE>

This means that the data is too large.

=item C<ENETWORK>

This means that an error in connection to Redis server was detected.

=item C<EMAXMEMORYLIMIT>

This means that the command is not allowed when used memory > C<maxmemory>
in the C<redis.conf> file.

=item C<EMAXMEMORYPOLICY>

This means that you are using the C<maxmemory-police all*> in the C<redis.conf> file.

=item C<ECOLLDELETED>

This means that the system part of the collection was removed prior to use.

=item C<EREDIS>

This means that other Redis error message detected.

=item C<EDATAIDEXISTS>

This means that you are trying to insert data with an ID that is already in
the data list.

=item C<EOLDERTHANALLOWED>

This means that you are trying to insert the data with the time less than
the time of the data that has been deleted from the data list.

=back

=head2 An Example

The example shows a possible treatment for possible errors.

    #-- Common ---------------------------------------------------------
    use Redis::CappedCollection qw(
        DEFAULT_SERVER
        DEFAULT_PORT

        ENOERROR
        EMISMATCHARG
        EDATATOOLARGE
        ENETWORK
        EMAXMEMORYLIMIT
        EMAXMEMORYPOLICY
        ECOLLDELETED
        EREDIS
        );

    # A possible treatment for possible errors
    sub exception {
        my $coll    = shift;
        my $err     = shift;

        die $err unless $coll;
        if ( $coll->last_errorcode == ENOERROR )
        {
            # For example, to ignore
            return unless $err;
        }
        elsif ( $coll->last_errorcode == EMISMATCHARG )
        {
            # Necessary to correct the code
        }
        elsif ( $coll->last_errorcode == EDATATOOLARGE )
        {
            # You must use the control data length
        }
        elsif ( $coll->last_errorcode == ENETWORK )
        {
            # For example, sleep
            #sleep 60;
            # and return code to repeat the operation
            #return 'to repeat';
        }
        elsif ( $coll->last_errorcode == EMAXMEMORYLIMIT )
        {
            # For example, return code to restart the server
            #return 'to restart the redis server';
        }
        elsif ( $coll->last_errorcode == EMAXMEMORYPOLICY )
        {
            # For example, return code to reinsert the data
            #return "to reinsert look at $err";
        }
        elsif ( $coll->last_errorcode == ECOLLDELETED )
        {
            # For example, return code to ignore
            #return "to ignore $err";
        }
        elsif ( $coll->last_errorcode == EREDIS )
        {
            # Independently analyze the $err
        }
        elsif ( $coll->last_errorcode == EDATAIDEXISTS )
        {
            # For example, return code to reinsert the data
            #return "to reinsert with new data ID";
        }
        elsif ( $coll->last_errorcode == EOLDERTHANALLOWED )
        {
            # Independently analyze the situation
        }
        else
        {
            # Unknown error code
        }
        die $err if $err;
    }

    my ( $list_id, $coll, @data );

    eval {
        $coll = Redis::CappedCollection->new(
            redis   => DEFAULT_SERVER.':'.DEFAULT_PORT,
            name    => 'Some name',
            size    => 100_000,
            );
    };
    exception( $coll, $@ ) if $@;
    print "'", $coll->name, "' collection created.\n";
    print 'Restrictions: ', $coll->size, ' size, "\n";

    #-- Producer -------------------------------------------------------
    #-- New data

    eval {
        $list_id = $coll->insert(
            'Some data stuff',
            'Some_id',      # If not specified, it creates
                            # a new items list named as UUID
            );
        print "Added data in a list with '", $list_id, "' id\n" );

        # Change the "zero" element of the list with the ID $list_id
        if ( $coll->update( $list_id, 0, 'Some new data stuff' ) )
        {
            print "Data updated successfully\n";
        }
        else
        {
            print "The data is not updated\n";
        }
    };
    exception( $coll, $@ ) if $@;

    #-- Consumer -------------------------------------------------------
    #-- Fetching the data

    eval {
        @data = $coll->receive( $list_id );
        print "List '$list_id' has '$_'\n" foreach @data;
        # or to obtain the data in the order they are received
        while ( my ( $list_id, $data ) = $coll->pop_oldest )
        {
            print "List '$list_id' had '$data'\n";
        }
    };
    exception( $coll, $@ ) if $@;

    #-- Utility --------------------------------------------------------
    #-- Getting statistics

    my ( $length, $lists, $items );
    eval {
    my $info = $coll->collection_info;
    print 'An existing collection uses ', $info->{length}, ' byte of data, ',
        'in ', $info->{items}, ' items are placed in ',
        $info->{lists}, ' lists', "\n";

        print "The collection has '$list_id' list\n"
            if $coll->exists( 'Some_id' );
    };
    exception( $coll, $@ ) if $@;

    #-- Closes and cleans up -------------------------------------------

    eval {
        $coll->quit;

        # Before use, make sure that the collection
        # is not being used by other customers
        #$coll->drop_collection;
    };
    exception( $coll, $@ ) if $@;

=head2 CappedCollection data structure

Using the currently selected database (default = 0).

While working on the Redis server creates and uses these data structures:

    #-- To store the collection status:
    # HASH    Namespace:status:Collection_id
    # For example:
    $ redis-cli
    redis 127.0.0.1:6379> KEYS Capped:status:*
    1) "Capped:status:89116152-C5BD-11E1-931B-0A690A986783"
    #      |      |                       |
    #  Namespace  |                       |
    #  Fixed symbol of a properties hash  |
    #                         Capped Collection id (UUID)
    ...
    redis 127.0.0.1:6379> HGETALL Capped:status:89116152-C5BD-11E1-931B-0A690A986783
    1) "size"                   # hash key
    2) "0"                      # the key value
    3) "length"                 # hash key
    4) "15"                     # the key value
    5) "lists"                  # hash key
    6) "1"                      # the key value
    7) "items"                  # hash key
    8) "1"                      # the key value
    9) "older_allowed"          # hash key
    10) "0"                     # the key value
    11) "big_data_threshold"    # hash key
    12) "0"                     # the key value

    #-- To store the collection queue:
    # ZSET    Namespace:queue:Collection_id
    # For example:
    redis 127.0.0.1:6379> KEYS Capped:queue:*
    1) "Capped:queue:89116152-C5BD-11E1-931B-0A690A986783"
    #      |     |                       |
    #  Namespace |                       |
    #  Fixed symbol of a queue           |
    #                        Capped Collection id (UUID)
    ...
    redis 127.0.0.1:6379> ZRANGE Capped:queue:89116152-C5BD-11E1-931B-0A690A986783 0 -1 WITHSCORES
    1) "478B9C84-C5B8-11E1-A2C5-D35E0A986783"
    2) "1348252575.6651001"|    |
    #           |          |    |
    #  Score: oldest data_time  |
    #                   Member: Data List id (UUID)
    ...

    #-- To store the CappedCollection data:
    # HASH    Namespace:I:Collection_id:DataList_id
    # HASH    Namespace:D:Collection_id:DataList_id
    # ZSET    Namespace:T:Collection_id:DataList_id
    # For example:
    redis 127.0.0.1:6379> KEYS Capped:?:*
    1) "Capped:I:89116152-C5BD-11E1-931B-0A690A986783:478B9C84-C5B8-11E1-A2C5-D35E0A986783"
    # If big_data_threshold = 0 or big_data_threshold > 0 and items on the list more than big_data_threshold
    2) "Capped:D:89116152-C5BD-11E1-931B-0A690A986783:478B9C84-C5B8-11E1-A2C5-D35E0A986783"
    3) "Capped:T:89116152-C5BD-11E1-931B-0A690A986783:478B9C84-C5B8-11E1-A2C5-D35E0A986783"
    #     |    |                     |                       |
    # Namespace|                     |                       |
    # Fixed symbol of a list of data |                       |
    #                    Capped Collection id (UUID)         |
    #                                                Data list id (UUID)
    ...
    redis 127.0.0.1:6379> HGETALL Capped:I:89116152-C5BD-11E1-931B-0A690A986783:478B9C84-C5B8-11E1-A2C5-D35E0A986783
    1) "n"                      # hash key (next_data_id)
    2) "1"                      # the key value
    3) "l"                      # hash key (last_removed_time)
    4) "0"                      # the key value
    5) "t"                      # hash key (oldest_time)
    6) "1348252575.5906"        # the key value
    7) "i"                      # hash key (oldest_data_id)
    8) "0"                      # the key value
    # If big_data_threshold > 0 and items on the list no more than big_data_threshold
    9) "0"                      # hash key
    10) "3"                     # the key value (items in the list)
    11) "1.i"                   # hash key
    12) "0"                     # the key value (Data id)
    13) "1.d"                   # hash key
    14) "Sume stuff"            # the key value (Data)
    15) "1.t"                   # hash key
    16) "1348252575.5906"       # the key value (data_time)
    ...
    # If big_data_threshold = 0 or big_data_threshold > 0 and items on the list more than big_data_threshold
    redis 127.0.0.1:6379> HGETALL Capped:D:89116152-C5BD-11E1-931B-0A690A986783:478B9C84-C5B8-11E1-A2C5-D35E0A986783
    1) "0"                      # hash key: Data id
    2) "Some stuff"             # the key value: Data
    ...
    # If big_data_threshold = 0 or big_data_threshold > 0 and items on the list more than big_data_threshold
    redis 127.0.0.1:6379> ZRANGE Capped:T:89116152-C5BD-11E1-931B-0A690A986783:478B9C84-C5B8-11E1-A2C5-D35E0A986783 0 -1 WITHSCORES
    1) "0" ---------------+
    2) "1348252575.5906"  |
    #           |         |
    #   Score: data_time  |
    #              Member: Data id
    ...

=head1 DEPENDENCIES

In order to install and use this package you will need Perl version
5.010 or better. The Redis::CappedCollection module depends on other
packages that are distributed separately from Perl. We recommend that
you have the following packages installed before you install
Redis::CappedCollection :

   Data::UUID
   Digest::SHA1
   Mouse
   Params::Util
   Redis

The Redis::CappedCollection module has the following optional dependencies:

   Test::Distribution
   Test::Exception
   Test::Kwalitee
   Test::Perl::Critic
   Test::Pod
   Test::Pod::Coverage
   Test::RedisServer
   Test::TCP

If the optional modules are missing, some "prereq" tests are skipped.

The installation of the missing dependencies can either be accomplished
through your OS package manager or through CPAN (or downloading the source
for all dependencies and compiling them manually).

=head1 BUGS AND LIMITATIONS

Need a Redis server version 2.6 or higher as module uses Redis Lua scripting.

The use of C<maxmemory-police all*> in the C<redis.conf> file could lead to
a serious (but hard to detect) problem as Redis server may delete
the collection element. Therefore the C<Redis::CappedCollection> does not work with
mode C<maxmemory-police all*> in the C<redis.conf>.

Full name of some Redis keys may not be known at the time of the call
the Redis Lua script (C<'EVAL'> or C<'EVALSHA'> command).
So the Redis server may not be able to correctly forward the request
to the appropriate node in the cluster.

We strongly recommend using the option C<maxmemory> in the C<redis.conf> file if
the data set may be large.

The C<Redis::CappedCollection> module was written, tested, and found working
on recent Linux distributions.

There are no known bugs in this package.

Please report problems to the L</"AUTHOR">.

Patches are welcome.

=head1 MORE DOCUMENTATION

All modules contain detailed information on the interfaces they provide.

=head1 SEE ALSO

The basic operation of the Redis::CappedCollection package module:

L<Redis::CappedCollection|Redis::CappedCollection> - Object interface to create
a collection, addition of data and data manipulation.

L<Redis|Redis> - Perl binding for Redis database.

=head1 AUTHOR

Sergey Gladkov, E<lt>sgladkov@trackingsoft.comE<gt>

=head1 CONTRIBUTORS

Alexander Solovey

Jeremy Jordan

Vlad Marchenko

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2012 by TrackingSoft LLC.
All rights reserved.

This package is free software; you can redistribute it and/or modify it under
the same terms as Perl itself. See I<perlartistic> at
L<http://dev.perl.org/licenses/artistic.html>.

This program is
distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE.

=cut
