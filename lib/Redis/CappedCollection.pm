package Redis::CappedCollection;

=head1 NAME

Redis::CappedCollection - Provides fixed size (determined by 'maxmemory'
Redis server setting) collections with FIFO data removal.

=head1 VERSION

This documentation refers to C<Redis::CappedCollection> version 1.05

=cut

#-- Pragmas --------------------------------------------------------------------

use 5.010;
use strict;
use warnings;
use bytes;

# ENVIRONMENT ------------------------------------------------------------------

our $VERSION = '1.05';

use Exporter qw(
    import
);

our @EXPORT_OK  = qw(
    $DATA_VERSION
    $DEFAULT_SERVER
    $DEFAULT_PORT
    $NAMESPACE
    $MIN_MEMORY_RESERVE
    $MAX_MEMORY_RESERVE

    $E_NO_ERROR
    $E_MISMATCH_ARG
    $E_DATA_TOO_LARGE
    $E_NETWORK
    $E_MAXMEMORY_LIMIT
    $E_MAXMEMORY_POLICY
    $E_COLLECTION_DELETED
    $E_REDIS
    $E_DATA_ID_EXISTS
    $E_OLDER_THAN_ALLOWED
    $E_NONEXISTENT_DATA_ID
    $E_INCOMP_DATA_VERSION
    $E_REDIS_DID_NOT_RETURN_DATA
    $E_UNKNOWN_ERROR
);

#-- load the modules -----------------------------------------------------------

use Carp;
use Const::Fast;
use Digest::SHA1 qw(
    sha1_hex
);
use List::Util qw(
    min
);
use Mouse;
use Mouse::Util::TypeConstraints;
use Params::Util qw(
    _ARRAY
    _ARRAY0
    _CLASSISA
    _INSTANCE
    _NONNEGINT
    _NUMBER
    _STRING
);
use Redis '1.976';
use Try::Tiny;

class_type 'Redis';
class_type 'Test::RedisServer';

#-- declarations ---------------------------------------------------------------

=head1 SYNOPSIS

    use 5.010;
    use strict;
    use warnings;

    #-- Common
    use Redis::CappedCollection qw(
        $DEFAULT_SERVER
        $DEFAULT_PORT
    );

    my $server = $DEFAULT_SERVER.':'.$DEFAULT_PORT;
    my $coll = Redis::CappedCollection->create( redis => { server => $server } );

    # Insert new data into collection
    my $list_id = $coll->insert( 'Some List_id', 'Some Data_id', 'Some data' );

    # Change the element of the list with the ID $list_id
    $updated = $coll->update( $list_id, $data_id, 'New data' );

    # Get data from a list with the ID $list_id
    @data = $coll->receive( $list_id );
    # or to obtain the data ordered from the oldest to the newest
    while ( my ( $list_id, $data ) = $coll->pop_oldest ) {
        say "List '$list_id' had '$data'";
    }

A brief example of the C<Redis::CappedCollection> usage is provided in
L</"An Example"> section.

The data structures used by C<Redis::CappedCollection> on Redis server
are explained in L</"CappedCollection data structure"> section.

=head1 ABSTRACT

Redis::CappedCollection module provides fixed sized collections that have
a auto-FIFO age-out feature.

The collection consists of multiple lists containing data items ordered
by time. Each list must have an unique ID within the collection and each
data item has unique ID within its list.

Automatic data removal (when size limit is reached) may remove the oldest
item from any list.

Collection size is determined by 'maxmemory' Redis server setting.

=head1 DESCRIPTION

Main features of the package are:

=over 3

=item *

Support creation of capped collection, status monitoring,
updating the data set, obtaining consistent data from the collection,
automatic data removal, error reporting.

=item *

Simple API for inserting and retrieving data and for managing collection.

=back

Capped collections are fixed-size collections that have an auto-FIFO
age-out feature based on the time of the inserted data. When collection
size reaches memory limit, the oldest data elements are removed automatically
to provide space for the new elements.

The lists in capped collection store their data items ordered by item time.

To insert a new data item into the capped collection, provide list ID, data ID,
data and optional data time (current time is used if not specified).
If there is a list with the given ID, the data is inserted into the existing list,
otherwise the new list is created automatically.

You may update the existing data in the collection, providing list ID, data ID and
optional data time. If no time is specified, the updated data will keep
its existing time.

Once the space is fully utilized, newly added data will replace
the oldest data in the collection.

Limits are specified when the collection is created.
Collection size is determined by 'maxmemory' redis server settings.

The package includes the utilities to dump and restore the collection:
F<dump_collection>, F<restore_collection> .

=head2 EXPORT

None by default.

Additional constants are available for import, which can be used
to define some type of parameters.

These are the defaults:

=head3 C<$DEFAULT_SERVER>

Default Redis local server: C<'localhost'>.

=cut
const our $DEFAULT_SERVER   => 'localhost';

=head3 C<$DEFAULT_PORT>

Default Redis server port: 6379.

=cut
const our $DEFAULT_PORT     => 6379;

=head3 C<$NAMESPACE>

Namespace name used keys on the Redis server: C<'C'>.

=cut
const our $NAMESPACE        => 'C';

=head3 C<$MIN_MEMORY_RESERVE>, C<$MAX_MEMORY_RESERVE>

Minimum and maximum memory reserve limits based on 'maxmemory'
configuration of the Redis server.

Not used when C<'maxmemory'> = 0 (not set in the F<redis.conf>).

The following values are used by default:

    $MIN_MEMORY_RESERVE = 0.05; # 5%
    $MAX_MEMORY_RESERVE = 0.5;  # 50%

=cut
const our $MIN_MEMORY_RESERVE   => 0.05;    # 5% memory reserve coefficient
const our $MAX_MEMORY_RESERVE   => 0.5;     # 50% memory reserve coefficient

=head3 C<$DATA_VERSION>

Current data structure version - 3.

=cut
const our $DATA_VERSION         => 3; # incremented for each incompatible data structure change

=over

=item Error codes

More details about error codes are provided in L</DIAGNOSTICS> section.

=back

Possible error codes:

=cut

=over 3

=item C<$E_NO_ERROR>

0 - No error

=cut
const our $E_NO_ERROR           => -1000;

=item C<$E_MISMATCH_ARG>

1 - Invalid argument.

Thrown by methods when there is a missing required argument or argument value is invalid.

=cut
const our $E_MISMATCH_ARG       => -1001;

=item C<$E_DATA_TOO_LARGE>

2 - Data is too large.

=cut
const our $E_DATA_TOO_LARGE     => -1002;

=item C<$E_NETWORK>

3 - Error in connection to Redis server.

=cut
const our $E_NETWORK            => -1003;

=item C<$E_MAXMEMORY_LIMIT>

4 - Command not allowed when used memory > 'maxmemory'.

This means that the command is not allowed when used memory > C<maxmemory>
in the F<redis.conf> file.

=cut
const our $E_MAXMEMORY_LIMIT    => -1004;

=item C<$E_MAXMEMORY_POLICY>

5 - Redis server have incompatible C<maxmemory-policy> setting.

Thrown when Redis server have incompatible C<maxmemory-policy> setting in F<redis.conf>.

=cut
const our $E_MAXMEMORY_POLICY   => -1005;

=item C<$E_COLLECTION_DELETED>

6 - Collection elements was removed prior to use.

This means that the system part of the collection was removed prior to use.

=cut
const our $E_COLLECTION_DELETED => -1006;

=item C<$E_REDIS>

7 - Redis error message.

This means that other Redis error message detected.

=cut
const our $E_REDIS              => -1007;

=item C<$E_DATA_ID_EXISTS>

8 - Attempt to add data with an existing ID

This means that you are trying to insert data with an ID that is already in
the data list.

=cut
const our $E_DATA_ID_EXISTS     => -1008;

=item C<$E_OLDER_THAN_ALLOWED>

9 - Attempt to add outdated data

This means that you are trying to insert the data with the time older than
the time of the oldest element currently stored in collection.

=cut
const our $E_OLDER_THAN_ALLOWED => -1009;

=item C<$E_NONEXISTENT_DATA_ID>

10 - Attempt to access the elements missing in the collection.

This means that you are trying to update data which does not exist.

=cut
const our $E_NONEXISTENT_DATA_ID    => -1010;

=item C<$E_INCOMP_DATA_VERSION>

11 - Attempt to access the collection with incompatible data structure, created
by an older or newer version of this module.

=cut
const our $E_INCOMP_DATA_VERSION    => -1011;

=item C<$E_REDIS_DID_NOT_RETURN_DATA>

12 - The Redis server did not return data.

Check the settings in the file F<redis.conf>.

=cut
const our $E_REDIS_DID_NOT_RETURN_DATA  => -1012;

=item C<$E_UNKNOWN_ERROR>

13 - Unknown error.

Possibly you should modify the constructor parameters for more intense automatic memory release.

=back

=cut
const our $E_UNKNOWN_ERROR          => -1013;

our %ERROR = (
    $E_NO_ERROR             => 'No error',
    $E_MISMATCH_ARG         => 'Invalid argument',
    $E_DATA_TOO_LARGE       => 'Data is too large',
    $E_NETWORK              => 'Error in connection to Redis server',
    $E_MAXMEMORY_LIMIT      => "Command not allowed when used memory > 'maxmemory'",
    $E_MAXMEMORY_POLICY     => 'Redis server have incompatible maxmemory-policy setting',
    $E_COLLECTION_DELETED   => 'Collection elements was removed prior to use',
    $E_REDIS                => 'Redis error message',
    $E_DATA_ID_EXISTS       => 'Attempt to add data to an existing ID',
    $E_OLDER_THAN_ALLOWED   => 'Attempt to add data over outdated',
    $E_NONEXISTENT_DATA_ID  => 'Non-existent data id',
    $E_INCOMP_DATA_VERSION  => 'Incompatible data version',
    $E_REDIS_DID_NOT_RETURN_DATA    => 'The Redis server did not return data',
    $E_UNKNOWN_ERROR        => 'Unknown error',
);

const our $REDIS_ERROR_CODE         => 'ERR';
const our $REDIS_MEMORY_ERROR_CODE  => 'OOM';
const our $REDIS_MEMORY_ERROR_MSG   => "$REDIS_MEMORY_ERROR_CODE $ERROR{ $E_MAXMEMORY_LIMIT }.";
const our $MAX_DATASIZE             => 512*1024*1024;   # A String value can be at max 512 Megabytes in length.
const my $MAX_REMOVE_RETRIES        => 2;       # the number of remove retries when memory limit is near
const my $USED_MEMORY_POLICY        => 'noeviction';

my $_lua_namespace  = "local NAMESPACE  = '".$NAMESPACE."'";
my $_lua_queue_key  = "local QUEUE_KEY  = NAMESPACE..':Q:'..coll_name";
my $_lua_status_key = "local STATUS_KEY = NAMESPACE..':S:'..coll_name";
my $_lua_data_keys  = "local DATA_KEYS  = NAMESPACE..':D:'..coll_name";
my $_lua_time_keys  = "local TIME_KEYS  = NAMESPACE..':T:'..coll_name";
my $_lua_data_key   = "local DATA_KEY   = DATA_KEYS..':'..list_id";
my $_lua_time_key   = "local TIME_KEY   = TIME_KEYS..':'..list_id";

my %lua_script_body;

my $_lua_clean_data = <<"END_CLEAN_DATA";
-- remove the control structures of the collection
if redis.call( 'EXISTS', QUEUE_KEY ) == 1 then
    ret = ret + redis.call( 'DEL', QUEUE_KEY )
end

-- each element of the list are deleted separately, as total number of items may be too large to send commands 'DEL'
$_lua_data_keys
$_lua_time_keys

local arr = redis.call( 'KEYS', DATA_KEYS..':*' )
if #arr > 0 then

-- remove structures store data lists
    for i = 1, #arr do
        ret = ret + redis.call( 'DEL', arr[i] )
    end

-- remove structures store time lists
    arr = redis.call( 'KEYS', TIME_KEYS..':*' )
    for i = 1, #arr do
        ret = ret + redis.call( 'DEL', arr[i] )
    end

end

return ret
END_CLEAN_DATA

my $_lua_cleaning = <<"END_CLEANING";
local MEMORY_RESERVE, MEMORY_RESERVE_COEFFICIENT, MAXMEMORY
local LAST_REDIS_USED_MEMORY = 0
local ROLLBACK = {}
local CLEANINGS = 0

local _DEBUG, _DEBUG_ID, _FUNC_NAME

local table_merge = function ( t1, t2 )
    for key, val in pairs( t2 ) do
        t1[ key ] = val
    end
end

local get_memory_used = function ()
    -- used_memory_lua included
    local redis_used_memory = string.match(
        redis.call( 'INFO', 'memory' ),
        'used_memory:(%d+)'
    )
    LAST_REDIS_USED_MEMORY = tonumber( redis_used_memory )
end

local _debug_log = function ( values )
    table_merge( values, {
        _DEBUG_ID                   = _DEBUG_ID,
        _FUNC_NAME                  = _FUNC_NAME,
        MAXMEMORY                   = MAXMEMORY,
        MEMORY_RESERVE              = MEMORY_RESERVE,
        MEMORY_RESERVE_COEFFICIENT  = MEMORY_RESERVE_COEFFICIENT,
        LAST_REDIS_USED_MEMORY      = LAST_REDIS_USED_MEMORY,
        list_id                     = list_id,
        data_id                     = data_id,
        data_len                    = #data,
        ROLLBACK                    = ROLLBACK
    } )

    redis.log( redis.LOG_NOTICE, _FUNC_NAME..': '..cjson.encode( values ) )
end

local _debug_switch = function ( argv_idx, func_name )
    if MEMORY_RESERVE == nil then   -- the first call
        MAXMEMORY       = tonumber( redis.call( 'CONFIG', 'GET', 'maxmemory' )[2] )
        MEMORY_RESERVE  = tonumber( redis.call( 'HGET', STATUS_KEY, 'memory_reserve' ) )
        if MAXMEMORY == 0 then
            MEMORY_RESERVE_COEFFICIENT = 0
        else
            MEMORY_RESERVE_COEFFICIENT = 1 + MEMORY_RESERVE
        end
    end

    if argv_idx ~= nil then
        _FUNC_NAME  = func_name
        _DEBUG_ID   = tonumber( ARGV[ argv_idx ] )
        if _DEBUG_ID ~= 0 then
            _DEBUG = true
            get_memory_used()
            _debug_log( {
                _STEP       = '_debug_switch',
                argv_idx    = argv_idx
            } )
        else
            _DEBUG = false
        end
    end
end

local cleaning_error = function ( error_msg )
    if _DEBUG then
        _debug_log( {
            _STEP       = 'cleaning_error',
            error_msg   = error_msg
        } )
    end

    for _, rollback_command in ipairs( ROLLBACK ) do
        redis.call( unpack( rollback_command ) )
    end
    -- Level 2 points the error to where the function that called error was called
    error( error_msg, 2 )
end

local table_val_to_str = function ( v )
    if "string" == type( v ) then
        v = string.gsub( v, "\\n", "\\\\n" )
        if string.match( string.gsub( v, "[^'\\"]", "" ), '^"+\$' ) then
            return "'" .. v .. "'"
        end
        return '"' .. string.gsub( v, '"', '\\\\"' ) .. '"'
    else
        return "table" == type( v ) and table_tostring( v ) or tostring( v )
    end
end

local table_key_to_str = function ( k )
    if "string" == type( k ) and string.match( k, "^[_%a][_%a%d]*\$" ) then
        return k
    else
        return "[" .. table_val_to_str( k ) .. "]"
    end
end

local table_tostring = function ( tbl )
    local result, done = {}, {}
    for k, v in ipairs( tbl ) do
        table.insert( result, table_val_to_str( v ) )
        done[ k ] = true
    end
    for k, v in pairs( tbl ) do
        if not done[ k ] then
            table.insert( result, table_key_to_str( k ) .. "=" .. table_val_to_str( v ) )
        end
    end
    return "{" .. table.concat( result, "," ) .. "}"
end

local call_with_error_control = function ( list_id, data_id, ... )
    local retries = $MAX_REMOVE_RETRIES
    local ret
    local error_msg = '<Empty error message>'
    repeat
        ret = redis.pcall( ... )
        if type( ret ) == 'table' and ret.err ~= nil then
            if _DEBUG then
                error_msg   = "$REDIS_MEMORY_ERROR_MSG - " .. ret.err .. " (call = " .. table_tostring( { ... } ) .. ")"
                _debug_log( {
                    _STEP       = 'call_with_error_control',
                    error_msg   = error_msg,
                    retries     = retries
                } )
            end

            cleaning( list_id, data_id, 1 )
        else
            break
        end
        retries = retries - 1
    until retries == 0

    if retries == 0 then
        -- Operation returned an error related to not enough memory.
        -- Start cleaning process and try to re-execute the operation.
        -- Repeat the cycle of operation + memory cleaning a couple of times and return an error / fail,
        -- if it still did not work.
        cleaning_error( error_msg )
    end

    return ret
end

local is_not_enough_memory = function ()
    if _DEBUG then
        _debug_log( { _STEP = 'is_not_enough_memory started' } )
    end

    if MEMORY_RESERVE_COEFFICIENT == 0 then
        return 0
    end

    if _DEBUG then
        _debug_log( { _STEP = 'is_not_enough_memory analysed' } )
    end

    if LAST_REDIS_USED_MEMORY * MEMORY_RESERVE_COEFFICIENT >= MAXMEMORY then
        if _DEBUG then
            _debug_log( {
                _STEP       = 'is_not_enough_memory'
            } )
        end

        return 1
    else
        return 0
    end
end

-- deleting old data to make room for new data
local cleaning = function ( list_id, data_id, is_forced_cleaning )
    local memory_reserve
    local enough_memory_cleaning_needed = 0

    get_memory_used()
    if is_forced_cleaning == 1 then
        enough_memory_cleaning_needed = 1
    else
        enough_memory_cleaning_needed = is_not_enough_memory()
    end

    if _DEBUG then
        _debug_log( {
            _STEP                           = 'Cleaning needed or not?',
            is_forced_cleaning              = is_forced_cleaning,
            enough_memory_cleaning_needed   = enough_memory_cleaning_needed
        } )
    end

    if enough_memory_cleaning_needed == 1 then
        local advance_cleanup_bytes = tonumber( redis.call( 'HGET', STATUS_KEY, 'advance_cleanup_bytes' ) )
        local advance_cleanup_num   = tonumber( redis.call( 'HGET', STATUS_KEY, 'advance_cleanup_num' ) )

        -- how much data to delete obsolete data
        local coll_items            = tonumber( redis.call( 'HGET', STATUS_KEY, 'items' ) )
        local advance_remaining_iterations = advance_cleanup_num
        if advance_remaining_iterations > coll_items then
            advance_remaining_iterations = coll_items
        end

        local total_items_deleted   = 0
        local advance_items_deleted = 0
        local lists_deleted         = 0
        local advance_bytes_deleted = 0
        local total_bytes_deleted   = 0
        local cleaning_iteration    = 1

        if _DEBUG then
            _debug_log( {
                _STEP                           = 'Before cleanings',
                is_forced_cleaning              = is_forced_cleaning,
                coll_items                      = coll_items,
                advance_cleanup_num             = advance_cleanup_num,
                advance_remaining_iterations    = advance_remaining_iterations,
                advance_cleanup_bytes           = advance_cleanup_bytes,
                advance_bytes_deleted           = advance_bytes_deleted,
                total_bytes_deleted             = total_bytes_deleted,
                enough_memory_cleaning_needed   = enough_memory_cleaning_needed
            } )
        end

        while
                coll_items > 0
            and (
                   advance_remaining_iterations > 0
                or ( advance_cleanup_bytes > 0 and advance_bytes_deleted < advance_cleanup_bytes )
                or enough_memory_cleaning_needed == 1
            )
        do
            if redis.call( 'EXISTS', QUEUE_KEY ) ~= 1 then
                -- Level 2 points the error to where the function that called error was called
                error( 'Queue key does not exist', 2 )
            end

            -- continue to work with the excess (requiring removal) data and for them using the prefix 'excess_'
            local excess_list_id, very_oldest_time = unpack( redis.call( 'ZRANGE', QUEUE_KEY, 0, 0, 'WITHSCORES' ) )
            very_oldest_time        = tonumber( very_oldest_time )
            -- key data structures
            local excess_data_key   = DATA_KEYS..':'..excess_list_id
            local excess_time_key   = TIME_KEYS..':'..excess_list_id

            -- looking for the oldest data
            local excess_data_id
            local excess_data
            local items = redis.call( 'HLEN', excess_data_key )
-- #FIXME: excess_data -> excess_data_len
-- HSTRLEN key field
-- Available since 3.2.0.
            if items == 1 then
                excess_data_id, excess_data = unpack( redis.call( 'HGETALL', excess_data_key ) )
            else
                excess_data_id   = redis.call( 'ZRANGE', excess_time_key, 0, 0 )[1]
                excess_data      = redis.call( 'HGET', excess_data_key, excess_data_id )
            end
            local excess_data_len = #excess_data
            excess_data = nil   -- free memory

            if _DEBUG then
                _debug_log( {
                    _STEP           = 'Before real cleaning',
                    items           = items,
                    excess_list_id  = excess_list_id,
                    excess_data_id  = excess_data_id,
                    excess_data_len = excess_data_len
                } )
            end

            if excess_list_id == list_id and excess_data_id == data_id then
                if cleaning_iteration == 1 then
                    -- Its first attempt to clean the conflicting data, for which the primary operation executed.
                    -- In this case, we are roll back operations that have been made before, and immediately return an error,
                    -- shifting the handling of such errors on the client.
                    cleaning_error( "$REDIS_MEMORY_ERROR_MSG" )
                end
                break
            end

            -- actually remove the oldest item
            if _DEBUG then
                local current_is_not_enough_memory                              = is_not_enough_memory()
                local advance_cleanup_bytes_yet_remains                         = advance_cleanup_bytes > 0 and advance_bytes_deleted < advance_cleanup_bytes

                local because_is_forced_cleaning                                = is_forced_cleaning
                local because_advance_cleanup_bytes                             = advance_cleanup_bytes_yet_remains

                local because_is_not_enough_memory                              = current_is_not_enough_memory == 1
                local because_advance_cleanup_num                               = advance_cleanup_num > 0
                local is_advance_remaining_iterations_yet_remains               = advance_remaining_iterations > 0
                local is_advance_remaining_iterations_truncated                 = advance_remaining_iterations < advance_cleanup_num
                local is_not_enough_memory_worked_out                           = current_is_not_enough_memory == 0
                local is_advance_remaining_iterations_worked_out                = advance_remaining_iterations <= 0
                local is_advance_cleanup_bytes_worked_out                       = not advance_cleanup_bytes_yet_remains
                local is_enough_memory_cleaning_before_advance_cleanup          =   total_items_deleted > 0
                                                                                and advance_items_deleted == 0
                                                                                and advance_bytes_deleted == 0
                local is_advance_cleanup_worked_after_enough_memory_cleaning    =   advance_items_deleted > 0
                                                                                and advance_bytes_deleted > 0
                                                                                and total_bytes_deleted > advance_bytes_deleted

                if because_advance_cleanup_bytes                            then because_advance_cleanup_bytes                          = 1 else because_advance_cleanup_bytes                          = 0 end
                if because_is_not_enough_memory                             then because_is_not_enough_memory                           = 1 else because_is_not_enough_memory                           = 0 end
                if because_advance_cleanup_num                              then because_advance_cleanup_num                            = 1 else because_advance_cleanup_num                            = 0 end
                if is_advance_remaining_iterations_yet_remains              then is_advance_remaining_iterations_yet_remains            = 1 else is_advance_remaining_iterations_yet_remains            = 0 end
                if is_advance_remaining_iterations_truncated                then is_advance_remaining_iterations_truncated              = 1 else is_advance_remaining_iterations_truncated              = 0 end
                if is_not_enough_memory_worked_out                          then is_not_enough_memory_worked_out                        = 1 else is_not_enough_memory_worked_out                        = 0 end
                if is_advance_remaining_iterations_worked_out               then is_advance_remaining_iterations_worked_out             = 1 else is_advance_remaining_iterations_worked_out             = 0 end
                if is_advance_cleanup_bytes_worked_out                      then is_advance_cleanup_bytes_worked_out                    = 1 else is_advance_cleanup_bytes_worked_out                    = 0 end
                if is_enough_memory_cleaning_before_advance_cleanup         then is_enough_memory_cleaning_before_advance_cleanup       = 1 else is_enough_memory_cleaning_before_advance_cleanup       = 0 end
                if is_advance_cleanup_worked_after_enough_memory_cleaning   then is_advance_cleanup_worked_after_enough_memory_cleaning = 1 else is_advance_cleanup_worked_after_enough_memory_cleaning = 0 end

                _debug_log( {
                    _STEP                                                   = 'Why it is cleared?',
                    coll_items                                              = coll_items,
                    advance_cleanup_bytes                                   = advance_cleanup_bytes,
                    advance_cleanup_num                                     = advance_cleanup_num,
                    enough_memory_cleaning_needed                           = enough_memory_cleaning_needed,
                    advance_remaining_iterations                            = advance_remaining_iterations,
                    total_items_deleted                                     = total_items_deleted,
                    advance_items_deleted                                   = advance_items_deleted,
                    advance_bytes_deleted                                   = advance_bytes_deleted,
                    total_bytes_deleted                                     = total_bytes_deleted,
                    because_is_forced_cleaning                              = because_is_forced_cleaning,
                    because_advance_cleanup_bytes                           = because_advance_cleanup_bytes,
                    because_is_not_enough_memory                            = because_is_not_enough_memory,
                    because_advance_cleanup_num                             = because_advance_cleanup_num,
                    is_advance_remaining_iterations_yet_remains             = is_advance_remaining_iterations_yet_remains,
                    is_advance_remaining_iterations_truncated               = is_advance_remaining_iterations_truncated,
                    is_not_enough_memory_worked_out                         = is_not_enough_memory_worked_out,
                    is_advance_remaining_iterations_worked_out              = is_advance_remaining_iterations_worked_out,
                    is_advance_cleanup_bytes_worked_out                     = is_advance_cleanup_bytes_worked_out,
                    is_enough_memory_cleaning_before_advance_cleanup        = is_enough_memory_cleaning_before_advance_cleanup,
                    is_advance_cleanup_worked_after_enough_memory_cleaning  = is_advance_cleanup_worked_after_enough_memory_cleaning
                } )
            end
            redis.call( 'HDEL', excess_data_key, excess_data_id )
            items = items - 1
            coll_items = coll_items - 1

            redis.call( 'HSET', STATUS_KEY, 'last_removed_time', very_oldest_time )

            if items > 0 then
                -- If the list has more data
                redis.call( 'ZREM', excess_time_key, excess_data_id )
                local oldest_time = tonumber( redis.call( 'ZRANGE', excess_time_key, 0, 0, 'WITHSCORES' )[2] )
                redis.call( 'ZADD', QUEUE_KEY, oldest_time, excess_list_id )

                if items == 1 then
                    redis.call( 'DEL', excess_time_key )
                end
            else
                -- If the list does not have data
                -- remove the name of the list from the queue collection
                redis.call( 'ZREM', QUEUE_KEY, excess_list_id )
                lists_deleted = lists_deleted + 1
            end
            get_memory_used()

            -- amount of data collection decreased
            total_items_deleted = total_items_deleted + 1
            total_bytes_deleted = total_bytes_deleted + excess_data_len
            if enough_memory_cleaning_needed == 0 then
                advance_bytes_deleted               = advance_bytes_deleted + excess_data_len
                advance_items_deleted               = advance_items_deleted + 1
                if advance_cleanup_num > 0 then
                    if advance_remaining_iterations > items then
                        advance_remaining_iterations = items
                    else
                        advance_remaining_iterations = advance_remaining_iterations - 1
                    end
                end
            end

            if _DEBUG then
                _debug_log( {
                    _STEP                           = 'After real cleaning',
                    excess_data_key                 = excess_data_key,
                    excess_data_id                  = excess_data_id,
                    coll_items                      = coll_items,
                    items                           = items,
                    total_items_deleted             = total_items_deleted,
                    advance_items_deleted           = advance_items_deleted,
                    advance_bytes_deleted           = advance_bytes_deleted,
                    total_bytes_deleted             = total_bytes_deleted,
                    advance_remaining_iterations    = advance_remaining_iterations,
                    cleaning_iteration              = cleaning_iteration
                } )
            end

            if enough_memory_cleaning_needed == 1 then
                enough_memory_cleaning_needed = is_not_enough_memory()
            end

            cleaning_iteration = cleaning_iteration + 1
            CLEANINGS = CLEANINGS + 1
        end

        if total_items_deleted ~= 0 then
            -- reduce the number of items in the collection
            redis.call( 'HINCRBY', STATUS_KEY, 'items', -total_items_deleted )
        end
        if lists_deleted ~= 0 then
            -- reduce the number of lists stored in a collection
            redis.call( 'HINCRBY',  STATUS_KEY, 'lists', -lists_deleted )
        end

        if _DEBUG then
            local is_enough_memory                          = is_not_enough_memory() == 0
            local is_memory_cleared                         = total_bytes_deleted > 0 and total_bytes_deleted > 0
            local is_advance_cleanup_bytes                  = advance_cleanup_bytes > 0
            local is_advance_cleanup_bytes_worked_out       = advance_bytes_deleted >= advance_cleanup_bytes
            local is_advance_cleanup_num                    = advance_cleanup_num > 0
            local is_advance_cleanup_num_worked_out         = advance_items_deleted >= advance_cleanup_num

            if is_enough_memory                             then is_enough_memory                           = 1 else is_enough_memory                           = 0 end
            if is_memory_cleared                            then is_memory_cleared                          = 1 else is_memory_cleared                          = 0 end
            if is_advance_cleanup_bytes                     then is_advance_cleanup_bytes                   = 1 else is_advance_cleanup_bytes                   = 0 end
            if is_advance_cleanup_bytes_worked_out          then is_advance_cleanup_bytes_worked_out        = 1 else is_advance_cleanup_bytes_worked_out        = 0 end
            if is_advance_cleanup_num                       then is_advance_cleanup_num                     = 1 else is_advance_cleanup_num                     = 0 end
            if is_advance_cleanup_num_worked_out            then is_advance_cleanup_num_worked_out          = 1 else is_advance_cleanup_num_worked_out          = 0 end

            _debug_log( {
                _STEP                                       = 'Cleaning finished',
                cleaning_iteration                          = cleaning_iteration,
                total_items_deleted                         = total_items_deleted,
                advance_items_deleted                       = advance_items_deleted,
                advance_bytes_deleted                       = advance_bytes_deleted,
                total_bytes_deleted                         = total_bytes_deleted,
                lists_deleted                               = lists_deleted,
                enough_memory_cleaning_needed               = enough_memory_cleaning_needed,
                advance_remaining_iterations                = advance_remaining_iterations,
                advance_cleanup_bytes                       = advance_cleanup_bytes,
                advance_cleanup_num                         = advance_cleanup_num,
                advance_bytes_deleted                       = advance_bytes_deleted,
                advance_items_deleted                       = advance_items_deleted,
                coll_items                                  = coll_items,
                is_forced_cleaning                          = is_forced_cleaning,
                is_enough_memory                            = is_enough_memory,
                is_memory_cleared                           = is_memory_cleared,
                is_advance_cleanup_bytes                    = is_advance_cleanup_bytes,
                is_advance_cleanup_bytes_worked_out         = is_advance_cleanup_bytes_worked_out,
                is_advance_cleanup_num                      = is_advance_cleanup_num,
                is_advance_cleanup_num_worked_out           = is_advance_cleanup_num_worked_out,
            } )
        end
    end
end
END_CLEANING

$lua_script_body{insert} = <<"END_INSERT";
-- adding data to a list of collections

local coll_name             = ARGV[1]
local list_id               = ARGV[2]
local data_id               = ARGV[3]
local data                  = ARGV[4]
local data_time             = tonumber( ARGV[5] )

-- key data storage structures
$_lua_namespace
$_lua_queue_key
$_lua_status_key
$_lua_data_keys
$_lua_time_keys
$_lua_data_key
$_lua_time_key

-- determine whether there is a list of data and a collection
if redis.call( 'EXISTS', STATUS_KEY ) ~= 1 then
    return { $E_COLLECTION_DELETED, 0 }
end

-- verification of the existence of old data with new data identifier
if redis.call( 'HEXISTS', DATA_KEY, data_id ) == 1 then
    return { $E_DATA_ID_EXISTS, 0 }
end

-- Validating the time of new data, if required
local last_removed_time = tonumber( redis.call( 'HGET', STATUS_KEY, 'last_removed_time' ) )

if redis.call( 'HGET', STATUS_KEY, 'older_allowed' ) ~= '1' then
    if redis.call( 'EXISTS', QUEUE_KEY ) == 1 then
        if data_time < last_removed_time then
            return { $E_OLDER_THAN_ALLOWED, 0 }
        end
    end
end

-- deleting obsolete data, if it is necessary
$_lua_cleaning
_debug_switch( 6, 'insert' )
cleaning( list_id, data_id, 0 )

-- add data to the list
-- Remember that the list and the collection can be automatically deleted after the "crowding out" old data

-- the existing data
local items = redis.call( 'HLEN', DATA_KEY )
local existing_id, existing_time
if items == 1 then
    existing_id   = redis.call( 'HGETALL', DATA_KEY )[1]
    existing_time = tonumber( redis.call( 'ZSCORE', QUEUE_KEY, list_id ) )
end

-- actually add data to the list
call_with_error_control( list_id, data_id, 'HSET', DATA_KEY, data_id, data )
data = nil  -- free memory
table.insert( ROLLBACK, 1, { 'HDEL', DATA_KEY, data_id } )

if redis.call( 'HLEN', DATA_KEY ) == 1 then  -- list recreated after cleaning
    redis.call( 'HINCRBY', STATUS_KEY, 'lists', 1 )
    table.insert( ROLLBACK, 1, { 'HINCRBY', STATUS_KEY, 'lists', -1 } )
    call_with_error_control( list_id, data_id, 'ZADD', QUEUE_KEY, data_time, list_id )
else
    if items == 1 then
        call_with_error_control( list_id, data_id, 'ZADD', TIME_KEY, existing_time, existing_id )
        table.insert( ROLLBACK, 1, { 'ZREM', TIME_KEY, existing_id } )
    end
    call_with_error_control( list_id, data_id, 'ZADD', TIME_KEY, data_time, data_id )
    local oldest_time = redis.call( 'ZRANGE', TIME_KEY, 0, 0, 'WITHSCORES' )[2]
    redis.call( 'ZADD', QUEUE_KEY, oldest_time, list_id )
end

-- reflect the addition of new data
redis.call( 'HINCRBY', STATUS_KEY, 'items', 1 )
if data_time < last_removed_time then
    redis.call( 'HSET', STATUS_KEY, 'last_removed_time', 0 )
end

return { $E_NO_ERROR, CLEANINGS }
END_INSERT

$lua_script_body{update} = <<"END_UPDATE";
-- update the data in the list of collections

local coll_name             = ARGV[1]
local list_id               = ARGV[2]
local data_id               = ARGV[3]
local data                  = ARGV[4]
local new_data_time         = tonumber( ARGV[5] )

-- key data storage structures
$_lua_namespace
$_lua_queue_key
$_lua_status_key
$_lua_data_keys
$_lua_time_keys
$_lua_data_key
$_lua_time_key

-- determine whether there is a list of data and a collection
if redis.call( 'EXISTS', STATUS_KEY ) ~= 1 then
    return { $E_COLLECTION_DELETED, 0 }
end
if redis.call( 'EXISTS', DATA_KEY ) ~= 1 then
    return { $E_NONEXISTENT_DATA_ID, 0 }
end
if redis.call( 'HEXISTS', DATA_KEY, data_id ) ~= 1 then
    return { $E_NONEXISTENT_DATA_ID, 0 }
end

local last_removed_time = tonumber( redis.call( 'HGET', STATUS_KEY, 'last_removed_time' ) )
if redis.call( 'HGET', STATUS_KEY, 'older_allowed' ) ~= '1' then
    if new_data_time ~= 0 and new_data_time < last_removed_time then
        return { $E_OLDER_THAN_ALLOWED, 0 }
    end
end

-- deleting obsolete data, if it can be necessary
$_lua_cleaning
_debug_switch( 6, 'update' )
cleaning( list_id, data_id, 0 )

-- data change
-- Remember that the list and the collection can be automatically deleted after the "crowding out" old data
if redis.call( 'HEXISTS', DATA_KEY, data_id ) ~= 1 then
    return { $E_NONEXISTENT_DATA_ID, 0 }
end

-- data to be changed were not removed

-- actually change
call_with_error_control( list_id, data_id, 'HSET', DATA_KEY, data_id, data )
data = nil  -- free memory

if new_data_time ~= 0 then
    if redis.call( 'HLEN', DATA_KEY ) == 1 then
        redis.call( 'ZADD', QUEUE_KEY, new_data_time, list_id )
    else
        redis.call( 'ZADD', TIME_KEY, new_data_time, data_id )
        local oldest_time = tonumber( redis.call( 'ZRANGE', TIME_KEY, 0, 0, 'WITHSCORES' )[2] )
        redis.call( 'ZADD', QUEUE_KEY, oldest_time, list_id )
    end

    if new_data_time < last_removed_time then
        redis.call( 'HSET', STATUS_KEY, 'last_removed_time', 0 )
    end
end

return { $E_NO_ERROR, CLEANINGS }
END_UPDATE

$lua_script_body{upsert} = <<"END_UPSERT";
-- update or insert the data in the list of collections

local coll_name             = ARGV[1]
local list_id               = ARGV[2]
local data_id               = ARGV[3]
local data_time             = tonumber( ARGV[5] )
local start_time            = ARGV[7]

-- key data storage structures
$_lua_namespace
$_lua_data_keys
$_lua_data_key

-- verification of the existence of old data with new data identifier
if redis.call( 'HEXISTS', DATA_KEY, data_id ) == 1 then
    if data_time == -1 then
        ARGV[5] = '0'
    end
    $lua_script_body{update}
else
    if data_time == -1 then
        ARGV[5] = start_time
    end
    $lua_script_body{insert}
end
END_UPSERT

$lua_script_body{receive} = <<"END_RECEIVE";
-- returns the data from the list

local coll_name             = ARGV[1]
local list_id               = ARGV[2]
local mode                  = ARGV[3]
local data_id               = ARGV[4]

-- key data storage structures
$_lua_namespace
$_lua_status_key
$_lua_data_keys
$_lua_data_key

-- determine whether there is a list of data and a collection
if redis.call( 'EXISTS', STATUS_KEY ) ~= 1 then
    -- sort of a mistake
    return nil
end

if mode == 'val' then
    -- returns the specified element of the data list
    return redis.call( 'HGET', DATA_KEY, data_id )
elseif mode == 'len' then
    -- returns the length of the data list
    return redis.call( 'HLEN', DATA_KEY )
elseif mode == 'vals' then
    -- returns all the data from the list
    return redis.call( 'HVALS', DATA_KEY )
elseif mode == 'all' then
    -- returns all data IDs and data values of the data list
    return redis.call( 'HGETALL', DATA_KEY )
else
    -- sort of a mistake
    return nil
end

END_RECEIVE

$lua_script_body{pop_oldest} = <<"END_POP_OLDEST";
-- retrieve the oldest data stored in the collection

local coll_name             = ARGV[1]

-- key data storage structures
$_lua_namespace
$_lua_queue_key
$_lua_status_key

-- determine whether there is a list of data and a collection
if redis.call( 'EXISTS', STATUS_KEY ) ~= 1 then
    -- sort of a mistake
    return { $E_COLLECTION_DELETED, nil, nil, nil }
end
if redis.call( 'EXISTS', QUEUE_KEY ) ~= 1 then
    return { $E_NO_ERROR, false, nil, nil }
end

-- initialize the data returned from the script
local list_exist    = 0
local list_id       = false
local data          = false

-- identifier of the list with the oldest data
list_id = redis.call( 'ZRANGE', QUEUE_KEY, 0, 0 )[1]

-- key data storage structures
$_lua_data_keys
$_lua_time_keys
$_lua_data_key
$_lua_time_key

-- determine whether there is a list of data and a collection
if redis.call( 'EXISTS', DATA_KEY ) ~= 1 then
    return { $E_COLLECTION_DELETED, nil, nil, nil }
end

-- Features the oldest data
local items = redis.call( 'HLEN', DATA_KEY )
local data_id
if items == 1 then
    data_id = redis.call( 'HGETALL', DATA_KEY )[1]
else
    data_id = redis.call( 'ZRANGE', TIME_KEY, 0, 0 )[1]
end
local very_oldest_time = tonumber( redis.call( 'ZRANGE', QUEUE_KEY, 0, 0, 'WITHSCORES' )[2] )

-- get data

-- actually get data
data = redis.call( 'HGET', DATA_KEY, data_id )

-- delete the data from the list
redis.call( 'HDEL', DATA_KEY, data_id )
items = items - 1

-- obtain information about the data that has become the oldest
local oldest_time = tonumber( redis.call( 'ZRANGE', TIME_KEY, 0, 0, 'WITHSCORES' )[2] )

if items > 0 then
    -- If the list has more data

    -- delete the information about the time of the data
    redis.call( 'ZREM', TIME_KEY, data_id )

    redis.call( 'ZADD', QUEUE_KEY, oldest_time, list_id )

    if items == 1 then
        -- delete the list data structure 'zset'
        redis.call( 'DEL', TIME_KEY )
    end
else
    -- if the list is no more data
    -- delete the list data structure 'zset'
    redis.call( 'DEL', TIME_KEY )

    -- reduce the number of lists stored in a collection
    redis.call( 'HINCRBY',  STATUS_KEY, 'lists', -1 )
    -- remove the name of the list from the queue collection
    redis.call( 'ZREM', QUEUE_KEY, list_id )
end

redis.call( 'HINCRBY', STATUS_KEY, 'items', -1 )
redis.call( 'HSET', STATUS_KEY, 'last_removed_time', very_oldest_time )

return { $E_NO_ERROR, true, list_id, data }
END_POP_OLDEST

$lua_script_body{collection_info} = <<"END_COLLECTION_INFO";
-- to obtain information on the status of the collection

local coll_name     = ARGV[1]

-- key data storage structures
$_lua_namespace
$_lua_queue_key
$_lua_status_key

-- determine whether there is a collection
if redis.call( 'EXISTS', STATUS_KEY ) ~= 1 then
    return { $E_COLLECTION_DELETED, false, false, false, false, false, false, false }
end

local oldest_time = redis.call( 'ZRANGE', QUEUE_KEY, 0, 0, 'WITHSCORES' )[2]
local lists, items, older_allowed, advance_cleanup_bytes, advance_cleanup_num, memory_reserve, data_version, last_removed_time = unpack( redis.call( 'HMGET', STATUS_KEY,
        'lists',
        'items',
        'older_allowed',
        'advance_cleanup_bytes',
        'advance_cleanup_num',
        'memory_reserve',
        'data_version',
        'last_removed_time'
    ) )

if type( data_version ) ~= 'string' then data_version = '0' end

return {
    $E_NO_ERROR,
    lists,
    items,
    older_allowed,
    advance_cleanup_bytes,
    advance_cleanup_num,
    memory_reserve,
    data_version,
    last_removed_time,
    oldest_time
}
END_COLLECTION_INFO

$lua_script_body{oldest_time} = <<"END_OLDEST_TIME";
-- to obtain time corresponding to the oldest data in the collection

local coll_name = ARGV[1]

-- key data storage structures
$_lua_namespace
$_lua_queue_key
$_lua_status_key

-- determine whe, falther there is a collection
if redis.call( 'EXISTS', STATUS_KEY ) ~= 1 then
    return { $E_COLLECTION_DELETED, false }
end

local oldest_time = redis.call( 'ZRANGE', QUEUE_KEY, 0, 0, 'WITHSCORES' )[2]
return { $E_NO_ERROR, oldest_time }
END_OLDEST_TIME

$lua_script_body{list_info} = <<"END_LIST_INFO";
-- to obtain information on the status of the data list

local coll_name             = ARGV[1]
local list_id               = ARGV[2]

-- key data storage structures
$_lua_namespace
$_lua_queue_key
$_lua_status_key
$_lua_data_keys
$_lua_data_key

-- determine whether there is a list of data and a collection
if redis.call( 'EXISTS', STATUS_KEY ) ~= 1 then
    return { $E_COLLECTION_DELETED, false, nil }
end
if redis.call( 'EXISTS', DATA_KEY ) ~= 1 then
    return { $E_NO_ERROR, false, nil }
end

-- the length of the data list
local items         = redis.call( 'HLEN', DATA_KEY )

-- the second data
local oldest_time   = redis.call( 'ZSCORE', QUEUE_KEY, list_id )

return { $E_NO_ERROR, items, oldest_time }
END_LIST_INFO

$lua_script_body{drop_collection} = <<"END_DROP_COLLECTION";
-- to remove the entire collection

local coll_name     = ARGV[1]

-- key data storage structures
$_lua_namespace
$_lua_queue_key
$_lua_status_key

-- initialize the data returned from the script
local ret = 0   -- the number of deleted items

if redis.call( 'EXISTS', STATUS_KEY ) == 1 then
    ret = ret + redis.call( 'DEL', STATUS_KEY )
end

$_lua_clean_data
END_DROP_COLLECTION

$lua_script_body{clear_collection} = <<"END_CLEAR_COLLECTION";
-- to remove the entire collection data

local coll_name     = ARGV[1]

-- key data storage structures
$_lua_namespace
$_lua_queue_key
$_lua_status_key

-- initialize the data returned from the script
local ret = 0   -- the number of deleted items

redis.call( 'HMSET', STATUS_KEY,
    'lists',                    0,
    'items',                    0,
    'last_removed_time',        0
);

$_lua_clean_data
END_CLEAR_COLLECTION

$lua_script_body{drop_list} = <<"END_DROP_LIST";
-- to remove the data_list

local coll_name     = ARGV[1]
local list_id       = ARGV[2]

-- key data storage structures
$_lua_namespace
$_lua_queue_key
$_lua_status_key
$_lua_data_keys
$_lua_data_key

-- determine whether there is a list of data and a collection
if redis.call( 'EXISTS', STATUS_KEY ) ~= 1 then
    return { $E_COLLECTION_DELETED, 0 }
end
if redis.call( 'EXISTS', DATA_KEY ) ~= 1 then
    return { $E_NO_ERROR, 0 }
end

-- initialize the data returned from the script
local ret = 0   -- the number of deleted items

-- key data storage structures
$_lua_time_keys
$_lua_time_key

-- determine the size of the data in the list and delete the list structure
local bytes_deleted = 0
local vals = redis.call( 'HVALS', DATA_KEY )
local list_items = #vals
for i = 1, list_items do
    bytes_deleted   = bytes_deleted + #vals[ i ]
end
redis.call( 'DEL', DATA_KEY, TIME_KEY )

-- reduce the number of items in the collection
redis.call( 'HINCRBY', STATUS_KEY, 'items', -list_items )
-- reduce the number of lists stored in a collection
redis.call( 'HINCRBY', STATUS_KEY, 'lists', -1 )
-- remove the name of the list from the queue collection
redis.call( 'ZREM', QUEUE_KEY, list_id )

return { $E_NO_ERROR, 1 }
END_DROP_LIST

$lua_script_body{verify_collection} = <<"END_VERIFY_COLLECTION";
-- creation of the collection and characterization of the collection by accessing existing collection

local coll_name             = ARGV[1];
local older_allowed         = ARGV[2];
local advance_cleanup_bytes = ARGV[3];
local advance_cleanup_num   = ARGV[4];
local memory_reserve        = ARGV[5];

local data_version = '$DATA_VERSION'

-- key data storage structures
$_lua_namespace
$_lua_status_key

-- determine whether there is a collection
local status_exist = redis.call( 'EXISTS', STATUS_KEY );

if status_exist == 1 then
-- if there is a collection
    older_allowed, advance_cleanup_bytes, advance_cleanup_num, memory_reserve, data_version = unpack( redis.call( 'HMGET', STATUS_KEY,
        'older_allowed',
        'advance_cleanup_bytes',
        'advance_cleanup_num',
        'memory_reserve',
        'data_version'
    ) );

    if type( data_version ) ~= 'string' then data_version = '0' end
else
-- if you want to create a new collection
    redis.call( 'HMSET', STATUS_KEY,
        'lists',                    0,
        'items',                    0,
        'older_allowed',            older_allowed,
        'advance_cleanup_bytes',    advance_cleanup_bytes,
        'advance_cleanup_num',      advance_cleanup_num,
        'memory_reserve',           memory_reserve,
        'data_version',             data_version,
        'last_removed_time',        0
    );
end

return {
    status_exist,
    older_allowed,
    advance_cleanup_bytes,
    advance_cleanup_num,
    memory_reserve,
    data_version
};
END_VERIFY_COLLECTION

$lua_script_body{_long_term_operation} = <<"END_LONG_TERM_OPERATION";
local coll_name         = ARGV[1];
local return_as_insert  = tonumber( ARGV[2] );

local STATUS_KEY            = 'C:S:'..coll_name;
local DATA_VERSION_KEY      = 'data_version';

local LIST                  = 'Test_list';
local DATA                  = 'Data';
local MAX_WORKING_CYCLES    = 5000000;

redis.call( 'DEL', LIST );

local ret;
local i = 1;
while i < MAX_WORKING_CYCLES do
    -- simple active actions
    local data_version = redis.call( 'HGET', STATUS_KEY, DATA_VERSION_KEY );
    ret = redis.call( 'HSET', LIST, i, DATA );

    i = i + 1;
end

if return_as_insert == 1 then
    return { $E_NO_ERROR, 0 };
else
    return { $E_NO_ERROR, ret, '_long_term_operation' };
end
END_LONG_TERM_OPERATION

subtype __PACKAGE__.'::NonNegInt',
    as 'Int',
    where { $_ >= 0 },
    message { ( $_ || '' ).' is not a non-negative integer!' }
;

subtype __PACKAGE__.'::NonEmptNameStr',
    as 'Str',
    where { $_ ne '' && $_ !~ /:/ },
    message { ( $_ || '' ).' is not a non-empty string!' }
;

subtype __PACKAGE__.'::DataStr',
    as 'Str',
    where { bytes::length( $_ ) <= $MAX_DATASIZE },
    message { "'".( $_ || '' )."' is not a valid data string!" }
;

#-- constructor ----------------------------------------------------------------

=head2 CONSTRUCTOR

=head3 C<create( redis =E<gt> $server, name =E<gt> $name, ... )>

Create a new collection on the Redis server and return an C<Redis::CappedCollection>
object to access it. Must be called as a class method only.

The C<create> creates and returns a C<Redis::CappedCollection> object that is configured
to work with the default settings if the corresponding arguments were not given.

C<redis> argument can be either an existing object of L<Redis|Redis> class
(which is then used for all communication with Redis server) or a hash reference used to create a
new internal Redis object. See documentation of L<Redis|Redis> module for details.

C<create> takes arguments in key-value pairs.

This example illustrates a C<create()> call with all the valid arguments:

    my $coll = Redis::CappedCollection->create(
        redis   => { server => "$server:$port" },   # Redis object
                        # or hash reference to parameters to create a new Redis object.
        name    => 'Some name', # Redis::CappedCollection collection name.
        advance_cleanup_bytes => 50_000, # The minimum size, in bytes,
                        # of the data to be released, if the size
                        # of the collection data after adding new data
                        # may exceed Redis server 'maxmemory' paramemter.
                        # Default 0 - additional data should not be released.
        advance_cleanup_num => 1_000, # Maximum number of data
                        # elements to delete, if the size
                        # of the collection data after adding new data
                        # may exceed 'maxmemory'.
                        # Default 0 - the number of times the deleted data
                        # is not limited.
        max_datasize    => 1_000_000,   # Maximum size, in bytes, of the data.
                        # Default 512MB.
        older_allowed   => 0, # Allow adding an element to collection that's older
                        # than the last element removed from collection.
                        # Default 0.
        check_maxmemory => 1, # Controls if collection should try to find out maximum
                        # available memory from Redis.
                        # In some cases Redis implementation forbids such request,
                        # but setting 'check_maxmemory' to false can be used
                        # as a workaround.
        memory_reserve  => 0,05,    # Reserve coefficient of 'maxmemory'.
                        # Not used when C<'maxmemory'> == 0 (it is not set in the F<redis.conf>).
                        # When you add or modify the data trying to ensure
                        # reserve of free memory for metadata and bookkeeping.
        reconnect_on_error = 0, # Controls ability to force re-connection with Redis on error.
    );

The C<redis> and C<name> arguments are required.
Do not use the symbol C<':'> in C<name>.

The following examples illustrate other uses of the C<create> method:

    my $redis = Redis->new( server => "$server:$port" );
    my $coll = Redis::CappedCollection->create( redis => $redis, name => 'Next collection' );
    my $next_coll = Redis::CappedCollection->create( redis => $coll, name => 'Some name' );

An error exception is thrown (C<confess>) if an argument is not valid or the collection with
same name already exists.

=cut
sub create {
    my $class = _CLASSISA( shift, __PACKAGE__ ) or confess 'Must be called as a class method only';
    return $class->new( @_, _create_from_naked_new => 0 );
}

sub BUILD {
    my $self = shift;

    my $redis = $self->redis;
    if ( _INSTANCE( $redis, 'Redis' ) ) {
        # have to look into the Redis object ...
        $self->_server( $redis->{server} );
        $self->_redis( $redis );
    } elsif ( _INSTANCE( $redis, 'Test::RedisServer' ) ) {
        # to test only
        # have to look into the Test::RedisServer object ...
        my $conf = $redis->conf;
        $conf->{server} = '127.0.0.1:'.$conf->{port} unless exists $conf->{server};
        $self->_server( $conf->{server} );
        $self->_redis( Redis->new( %$conf ) );
    } elsif ( _INSTANCE( $redis, __PACKAGE__ ) ) {
        $self->_server( $redis->_server );
        $self->_redis( $self->_redis );
    } else {    # $redis is hash ref
        $self->_server( $redis->{server} // "$DEFAULT_SERVER:$DEFAULT_PORT" );

        # defaults for the case when the Redis object we create
        $redis->{reconnect}                 = 0     unless exists $redis->{reconnect};
        $redis->{every}                     = 1000  unless exists $redis->{every};                  # 1 ms
        $redis->{conservative_reconnect}    = 0     unless exists $redis->{conservative_reconnect};
        $redis->{cnx_timeout}               = 0.1   unless exists $redis->{cnx_timeout};
        $redis->{read_timeout}              = 1     unless exists $redis->{read_timeout};
        $redis->{write_timeout}             = 1     unless exists $redis->{write_timeout};

        $self->_redis( $self->_redis_constructor( $redis ) );
        $self->_use_external_connection( 0 );
    }

    if ( $self->_create_from_naked_new ) {
        warn 'Redis::CappedCollection->new() is deprecated and will be removed in future. Please use either create() or open() instead.';
    } else {
        confess "Collection '".$self->name."' already exists"
            if !$self->_create_from_open && $self->collection_exists( name => $self->name );
    }

    my $maxmemory;
    if ( $self->_check_maxmemory ) {
        ( undef, $maxmemory ) = $self->_call_redis( 'CONFIG', 'GET', 'maxmemory' );
        defined( _NONNEGINT( $maxmemory ) )
            or $self->_throw( $E_NETWORK );
    } else {
        # 0 means all system memory
        $maxmemory = 0;
    }

    my ( $major, $minor ) = $self->_redis->info->{redis_version} =~ /^(\d+)\.(\d+)/;
    if ( $major < 2 || ( $major == 2 && $minor < 8 ) ) {
        $self->_set_last_errorcode( $E_REDIS );
        confess "Need a Redis server version 2.8 or higher";
    }

    $self->_throw( $E_MAXMEMORY_POLICY )
        unless $self->_maxmemory_policy_ok;

    $self->_maxmemory( $maxmemory );
    $self->max_datasize( min $self->_maxmemory, $self->max_datasize )
        if $self->_maxmemory;

    $self->_queue_key(  $NAMESPACE.':Q:'.$self->name );
    $self->_status_key( _make_status_key( $self->name ) );
    $self->_data_keys(  _make_data_key( $self->name ) );
    $self->_time_keys(  $NAMESPACE.':T:'.$self->name );

    $self->_verify_collection unless $self->_create_from_open;
}

#-- public attributes ----------------------------------------------------------

=head3 C<open( redis =E<gt> $server, name =E<gt> $name, ... )>

    my $redis = Redis->new( server => "$server:$port" );
    my $coll = Redis::CappedCollection::open( redis => $redis, name => 'Some name' );

Create a C<Redis::CappedCollection> object to work with an existing collection
(created by L</create>). It must be called as a class method only.

C<open> takes optional arguments. These arguments are in key-value pairs.
Arguments description is the same as for L</create> method.

=over 3

=item I<redis>

=item I<name>

=item I<max_datasize>

=item I<check_maxmemory>

=back

The C<redis> and C<name> arguments are mandatory.

The C<open> creates and returns a C<Redis::CappedCollection> object that is configured
to work with the default settings if the corresponding arguments are not given.

If C<redis> argument is not a L<Redis> object, a new connection to Redis is established using
passed hash reference to create a new L<Redis> object.

An error exception is thrown (C<confess>) if an argument is not valid.

=cut
my @_asked_parameters = qw(
    redis
    name
    max_datasize
    check_maxmemory
);
my @_status_parameters = qw(
    older_allowed
    advance_cleanup_bytes
    advance_cleanup_num
    memory_reserve
);

sub open {
    my $class = _CLASSISA( shift, __PACKAGE__ ) or confess 'Must be called as a class method only';

    my %params = @_;
    _check_arguments_acceptability( \%params, \@_asked_parameters );

    confess "'redis' argument is required"  unless exists $params{redis};
    confess "'name' argument is required"   unless exists $params{name};

    my $use_external_connection = ref( $params{redis} ) ne 'HASH';
    my $redis   = $params{redis} = _get_redis( $params{redis} );
    my $name    = $params{name};
    if ( collection_exists( redis => $redis, name => $name ) ) {
        my $info = collection_info( redis => $redis, name => $name );
        $info->{data_version} == $DATA_VERSION or confess $ERROR{ $E_INCOMP_DATA_VERSION };
        $params{ $_ } = $info->{ $_ } foreach @_status_parameters;
        return $class->new( %params,
            _create_from_naked_new      => 0,
            _create_from_open           => 1,
            _use_external_connection    => $use_external_connection,
        );
    } else {
        confess "Collection '$name' does not exist";
    };
}

=head2 METHODS

An exception is thrown (C<confess>) if any method argument is not valid or
if a required argument is missing.

ATTENTION: In the L<Redis|Redis> module the synchronous commands throw an
exception on receipt of an error reply, or return a non-error reply directly.

=cut

=head3 C<name>

Get collection C<name> attribute (collection ID).
The method returns the current value of the attribute.
The C<name> attribute value is used in the L<constructor|/CONSTRUCTOR>.

=cut
has name                   => (
    is          => 'ro',
    clearer     => '_clear_name',
    isa         => __PACKAGE__.'::NonEmptNameStr',
    required    => 1,
);

=head3 C<redis>

Existing L<Redis> object or a hash reference with parameters to create a new one.

=cut
has redis                   => (
    is          => 'ro',
    isa         => 'Redis|Test::RedisServer|HashRef',
    required    => 1,
);

=head3 C<reconnect_on_error>

Controls ability to force re-connection with Redis on error.

=cut
has reconnect_on_error      => (
    is          => 'rw',
    isa         => 'Bool',
    default     => 0,
);

=head3 C<advance_cleanup_bytes>

Accessor for C<advance_cleanup_bytes> attribute - the minimum size,
in bytes, of the data to be released in addition to memory reserve, if the size
of the collection data after adding new data may exceed C<'maxmemory'>. Default
is C<0> - additional data should not be released.

The C<advance_cleanup_bytes> attribute is designed to reduce the release of memory
operations with frequent data changes.

The C<advance_cleanup_bytes> attribute value can be provided to L</create>.
The method returns and sets the current value of the attribute.

The C<advance_cleanup_bytes> value must be less than or equal to C<'maxmemory'>. Otherwise
an error exception is thrown (C<confess>).

=cut
has advance_cleanup_bytes   => (
    is          => 'rw',
    writer      => '_set_advance_cleanup_bytes',
    isa         => __PACKAGE__.'::NonNegInt',
    default     => 0,
    trigger     => sub {
        my $self = shift;
        !$self->_maxmemory || ( $self->advance_cleanup_bytes <= $self->maxmemory || $self->_throw( $E_MISMATCH_ARG, 'advance_cleanup_bytes' ) );
    },
);

=head3 C<advance_cleanup_num>

The method of access to the C<advance_cleanup_num> attribute - maximum number
of data elements to delete, if the size of the collection data after adding
new data may exceed C<'maxmemory'>. Default 0 - the number of times the deleted data
is not limited.

The C<advance_cleanup_num> attribute is designed to reduce the number of
deletes data.

The C<advance_cleanup_num> attribute value can be used in the L<constructor|/CONSTRUCTOR>.
The method returns and sets the current value of the attribute.

=cut
has advance_cleanup_num     => (
    is          => 'rw',
    writer      => '_set_advance_cleanup_num',
    isa         => __PACKAGE__.'::NonNegInt',
    default     => 0,
);

=head3 C<max_datasize>

Accessor for the C<max_datasize> attribute.

The method returns the current value of the attribute if called without arguments.

Non-negative integer value can be used to specify a new value to
the maximum size of the data introduced into the collection
(methods L</insert> and L</update>).

The C<max_datasize> attribute value is used in the L<constructor|/CONSTRUCTOR>
and operations data entry on the Redis server.

The L<constructor|/CONSTRUCTOR> uses the smaller of the values of 512MB and
C<'maxmemory'> limit from a F<redis.conf> file.

=cut
has max_datasize            => (
    is          => 'rw',
    isa         => __PACKAGE__.'::NonNegInt',
    default     => $MAX_DATASIZE,
    lazy        => 1,
    trigger     => sub {
        my $self = shift;
        $self->max_datasize <= ( $self->_maxmemory ? min( $self->_maxmemory, $MAX_DATASIZE ) : $MAX_DATASIZE )
            || $self->_throw( $E_MISMATCH_ARG, 'max_datasize' );
    },
);

=head3 C<older_allowed>

Accessor for the C<older_allowed> attribute which controls if adding an element
that is older than the last element removed from collection is allowed.
Default is C<0> (not allowed).

The method returns the current value of the attribute.
The C<older_allowed> attribute value is used in the L<constructor|/CONSTRUCTOR>.

=cut
has older_allowed           => (
    is          => 'rw',
    isa         => 'Bool',
    default     => 0,
);

=head3 C<memory_reserve>

Accessor for the C<memory_reserve> attribute which specifies the amount of additional
memory reserved for metadata and bookkeeping.
Default C<0.05> (5%) of 'maxmemory'.
Not used when C<'maxmemory'> == 0 (it is not set in the F<redis.conf>).

Valid values must be between C<$MIN_MEMORY_RESERVE> and C<$MAX_MEMORY_RESERVE>.

The method returns the current value of the attribute.
The C<memory_reserve> attribute value is used in the L<constructor|/CONSTRUCTOR>.

=cut
has memory_reserve          => (
    is          => 'rw',
    writer      => '_set_memory_reserve',
    isa         => 'Num',
    default     => $MIN_MEMORY_RESERVE,
    trigger     => sub {
        my $self = shift;
        my $memory_reserve = $self->memory_reserve;
        ( _NUMBER( $memory_reserve ) && $memory_reserve >= $MIN_MEMORY_RESERVE && $memory_reserve <= $MAX_MEMORY_RESERVE )
                || $self->_throw( $E_MISMATCH_ARG, 'memory_reserve' );
    },
);

=head3 C<last_errorcode>

Get code of the last error.

See the list of supported error codes in L</DIAGNOSTICS> section.

=cut
has last_errorcode          => (
    reader      => 'last_errorcode',
    writer      => '_set_last_errorcode',
    isa         => 'Int',
    default     => 0,
);

#-- public methods -------------------------------------------------------------

=head3 C<insert( $list_id, $data_id, $data, $data_time )>

    $list_id = $coll->insert( 'Some List_id', 'Some Data_id', 'Some data' );

    $list_id = $coll->insert( 'Another List_id', 'Data ID', 'More data', Time::HiRes::time() );

Insert data into the capped collection on the Redis server.

Arguments:

=over 3

=item C<$list_id>

Mandatory, non-empty string: list ID. Must not contain C<':'>.

The data will be inserted into the list with given ID, and the list
is created automatically if it does not exist yet.

=item C<$data_id>

Mandatory, non-empty string: data ID, unique within the list identified by C<$list_id>
argument.

=item C<$data>

Data value: a string. Data length should not exceed value of L</max_datasize> attribute.

=item C<$data_time>

Optional data time, a non-negative number. If not specified, the current
value returned by C<time()> is used instead. Floating values (such as those
returned by L<Time::HiRes|Time::HiRes> module) are supported to have time
granularity of less than 1 second and stored with 4 decimal places.

=back

If collection is set to C<older_allowed == 1> and C<$data_time> less than time of the last removed
element (C<last_removed_time> - see L</collection_info>) then C<last_removed_time> is set to 0.
The C<older_allowed> attribute value is used in the L<constructor|/CONSTRUCTOR>.

The method returns the ID of the data list to which the data was inserted (value of
the C<$list_id> argument).

=cut
sub insert {
    my $self        = shift;
    my $list_id     = shift;
    my $data_id     = shift;
    my $data        = shift;
    my $data_time   = shift // time;

    $data                                                   // $self->_throw( $E_MISMATCH_ARG, 'data' );
    ( defined( _STRING( $data ) ) || $data eq '' )          || $self->_throw( $E_MISMATCH_ARG, 'data' );
    _STRING( $list_id )                                     // $self->_throw( $E_MISMATCH_ARG, 'list_id' );
    $list_id !~ /:/                                         || $self->_throw( $E_MISMATCH_ARG, 'list_id' );
    defined( _STRING( $data_id ) )                          || $self->_throw( $E_MISMATCH_ARG, 'data_id' );
    ( defined( _NUMBER( $data_time ) ) && $data_time > 0 )  || $self->_throw( $E_MISMATCH_ARG, 'data_time' );

    my $data_len = bytes::length( $data );
    ( $data_len <= $self->max_datasize )                    || $self->_throw( $E_DATA_TOO_LARGE );

    $self->_set_last_errorcode( $E_NO_ERROR );

    my @ret = $self->_call_redis(
        $self->_lua_script_cmd( 'insert' ),
        0,
        $self->name,
        $list_id,
        $data_id,
        $data,
        $data_time,
        # Recommend the inclusion of this option in the case of incomprehensible errors
        $self->_DEBUG,
    );

    my ( $error, $cleanings ) = @ret;

    if ( scalar( @ret ) == 2 && exists( $ERROR{ $error } ) && defined( _NONNEGINT( $cleanings ) ) ) {
        if ( $error == $E_NO_ERROR ) {
            # Normal result: Nothing to do
        } elsif ( $error == $E_COLLECTION_DELETED ) {
            $self->_clear_sha1;
            $self->_throw( $error );
        } elsif (
                   $error == $E_DATA_ID_EXISTS
                || $error == $E_OLDER_THAN_ALLOWED
            ) {
            $self->_throw( $error );
        } else {
            $self->_throw( $error, 'Unexpected error' );
        }
    } else {
        $self->_process_unknown_error( @ret );
    }

    return wantarray ? ( $list_id, $cleanings ) : $list_id;
}

=head3 C<update( $list_id, $data_id, $data, $new_data_time )>

    if ( $coll->update( $list_id, $data_id, 'New data' ) ) {
        say "Data updated successfully";
    } else {
        say "The data is not updated";
    }

Updates existing data item.

Arguments:

=over 3

=item C<$list_id>

Mandatory, non-empty string: list ID. Must not contain C<':'>.

=item C<$data_id>

Mandatory, non-empty string: data ID, unique within the list identified by C<$list_id>
argument.

=item C<$data>

New data value: a string. Data length should not exceed value of L</max_datasize> attribute.

=item C<$new_data_time>

Optional new data time, a non-negative number. If not specified, the existing
data time is preserved.

=back

If the collection is set to C<older_allowed == 1> and C<$new_data_time> less than time of the last
removed element (C<last_removed_time> - see L</collection_info>) then C<last_removed_time> is set to 0.
The C<older_allowed> attribute value is used in the L<constructor|/CONSTRUCTOR>.

Method returns true if the data is updated or false if the list with the given ID does not exist or
is used an invalid data ID.

Throws an exception on other errors.

=cut
sub update {
    my $self    = shift;
    my $list_id = shift;
    my $data_id = shift;
    my $data    = shift;

    $data                                           // $self->_throw( $E_MISMATCH_ARG, 'data' );
    ( defined( _STRING( $data ) ) || $data eq '' )  || $self->_throw( $E_MISMATCH_ARG, 'data' );
    _STRING( $list_id )                             // $self->_throw( $E_MISMATCH_ARG, 'list_id' );
    defined( _STRING( $data_id ) )                  || $self->_throw( $E_MISMATCH_ARG, 'data_id' );

    my $new_data_time;
    if ( @_ ) {
        $new_data_time = shift;
        ( defined( _NUMBER( $new_data_time ) ) && $new_data_time > 0 ) || $self->_throw( $E_MISMATCH_ARG, 'new_data_time' );
    }

    my $data_len = bytes::length( $data );
    ( $data_len <= $self->max_datasize )            || $self->_throw( $E_DATA_TOO_LARGE );

    $self->_set_last_errorcode( $E_NO_ERROR );

    my @ret = $self->_call_redis(
        $self->_lua_script_cmd( 'update' ),
        0,
        $self->name,
        $list_id,
        $data_id,
        $data,
        $new_data_time // 0,
        # Recommend the inclusion of this option in the case of incomprehensible errors
        $self->_DEBUG,
    );

    my ( $error, $cleanings ) = @ret;

    if ( scalar( @ret ) == 2 && exists( $ERROR{ $error } ) && defined( _NONNEGINT( $cleanings ) ) ) {
        if ( $error == $E_NO_ERROR ) {
            return wantarray ? ( 1, $cleanings ) : 1;
        } elsif ( $error == $E_NONEXISTENT_DATA_ID ) {
            return 0;
        } elsif (
                   $error == $E_COLLECTION_DELETED
                || $error == $E_DATA_ID_EXISTS
                || $error == $E_OLDER_THAN_ALLOWED
            ) {
            $self->_clear_sha1;
            $self->_throw( $error );
        } else {
            $self->_throw( $error, 'Unexpected error' );
        }
    } else {
        $self->_process_unknown_error( @ret );
    }
}

=head3 C<upsert( $list_id, $data_id, $data, $data_time )>

    $list_id = $coll->upsert( 'Some List_id', 'Some Data_id', 'Some data' );

    $list_id = $coll->upsert( 'Another List_id', 'Data ID', 'More data', Time::HiRes::time() );

If the list C<$list_id> does not contain data with C<$data_id>,
then it behaves like an L</insert>,
otherwise behaves like an L</update>.

The method returns the ID of the data list to which the data was inserted (value of
the C<$list_id> argument) as the L</insert> method.

=cut
sub upsert {
    my $self        = shift;
    my $list_id     = shift;
    my $data_id     = shift;
    my $data        = shift;
    my $data_time   = shift;

    $data                                                   // $self->_throw( $E_MISMATCH_ARG, 'data' );
    ( defined( _STRING( $data ) ) || $data eq '' )          || $self->_throw( $E_MISMATCH_ARG, 'data' );
    _STRING( $list_id )                                     // $self->_throw( $E_MISMATCH_ARG, 'list_id' );
    $list_id !~ /:/                                         || $self->_throw( $E_MISMATCH_ARG, 'list_id' );
    defined( _STRING( $data_id ) )                          || $self->_throw( $E_MISMATCH_ARG, 'data_id' );
    !defined( $data_time ) || ( defined( _NUMBER( $data_time ) ) && $data_time > 0 ) || $self->_throw( $E_MISMATCH_ARG, 'data_time' );

    my $data_len = bytes::length( $data );
    ( $data_len <= $self->max_datasize )                    || $self->_throw( $E_DATA_TOO_LARGE );

    $self->_set_last_errorcode( $E_NO_ERROR );

    my @ret = $self->_call_redis(
        $self->_lua_script_cmd( 'upsert' ),
        0,
        $self->name,
        $list_id,
        $data_id,
        $data,
        $data_time // -1,
        # Recommend the inclusion of this option in the case of incomprehensible errors
        $self->_DEBUG,
        time,
    );

    my ( $error, $cleanings ) = @ret;

    if ( scalar( @ret ) == 2 && exists( $ERROR{ $error } ) && defined( _NONNEGINT( $cleanings ) ) ) {
        if ( $error == $E_NO_ERROR ) {
            # Normal result: Nothing to do
        } elsif ( $error == $E_COLLECTION_DELETED ) {
            $self->_clear_sha1;
            $self->_throw( $error );
        } elsif (
                   $error == $E_DATA_ID_EXISTS
                || $error == $E_OLDER_THAN_ALLOWED
            ) {
            $self->_throw( $error );
        } elsif ( $error == $E_NONEXISTENT_DATA_ID ) {
            # Nothing to do
        } else {
            $self->_throw( $error, 'Unexpected error' );
        }
    } else {
        $self->_process_unknown_error( @ret );
    }

    return wantarray ? ( $list_id, $cleanings ) : $list_id; # as insert
}

=head3 C<receive( $list_id, $data_id )>

    my @data = $coll->receive( $list_id );
    say "List '$list_id' has '$_'" foreach @data;
    # or
    my $list_len = $coll->receive( $list_id );
    say "List '$list_id' has '$list_len' item(s)";
    # or
    my $data = $coll->receive( $list_id, $data_id );
    say "List '$list_id' has '$data_id'" if defined $data;

If the C<$data_id> argument is not specified or is an empty string:

=over 3

=item *

In a list context, the method returns all the data from the list given by
the C<$list_id> identifier.

Method returns an empty list if the list with the given ID does not exist.

=item *

In a scalar context, the method returns the length of the data list given by
the C<$list_id> identifier.

=back

If the C<$data_id> argument is specified:

=over 3

=item *

The method returns the specified element of the data list.
If the data with C<$data_id> ID does not exist, C<undef> is returned.

=back

=cut
sub receive {
    my ( $self, $list_id, $data_id ) = @_;

    _STRING( $list_id ) // $self->_throw( $E_MISMATCH_ARG, 'list_id' );

    $self->_set_last_errorcode( $E_NO_ERROR );

    return unless $self->list_exists( $list_id );

    if ( defined( $data_id ) && $data_id ne '' ) {
        _STRING( $data_id ) // $self->_throw( $E_MISMATCH_ARG, 'data_id' );
        return $self->_call_redis(
            $self->_lua_script_cmd( 'receive' ),
            0,
            $self->name,
            $list_id,
            'val',
            $data_id,
        );
    } else {
        if ( wantarray ) {
            return $self->_call_redis(
                $self->_lua_script_cmd( 'receive' ),
                0,
                $self->name,
                $list_id,
                defined( $data_id ) ? 'all' : 'vals',
                '',
            );
        } else {
            return $self->_call_redis(
                $self->_lua_script_cmd( 'receive' ),
                0,
                $self->name,
                $list_id,
                'len',
                '',
            );
        }
    }
}

=head3 C<pop_oldest>

The method retrieves the oldest data stored in the collection and removes it from
the collection.

Returns a list of two elements.
The first element contains the identifier of the list from which the data was retrieved.
The second element contains the extracted data.

The returned data item is removed from the collection.

Method returns an empty list if the collection does not contain any data.

The following examples illustrate uses of the C<pop_oldest> method:

    while ( my ( $list_id, $data ) = $coll->pop_oldest ) {
        say "List '$list_id' had '$data'";
    }

=cut
sub pop_oldest {
    my ( $self ) = @_;

    $self->_set_last_errorcode( $E_NO_ERROR );

    my @ret = $self->_call_redis(
        $self->_lua_script_cmd( 'pop_oldest' ),
        0,
        $self->name,
    );

    my ( $error, $queue_exist, $excess_id, $excess_data ) = @ret;

    if ( exists $ERROR{ $error } ) {
        $self->_clear_sha1 if $error == $E_COLLECTION_DELETED;
        $self->_throw( $error ) if $error != $E_NO_ERROR;
    } else {
        $self->_process_unknown_error( @ret );
    }

    if ( $queue_exist ) {
        return( $excess_id, $excess_data );
    } else {
        return;
    }
}

=head3 C<redis_config_ok( redis =E<gt> $server )>

    say 'Redis server config ', $coll->redis_config_ok ? 'OK' : 'NOT OK';
    my $redis = Redis->new( server => "$server:$port" );
    say 'Redis server config ',
        Redis::CappedCollection::redis_config_ok( redis => $redis )
            ? 'OK'
            : 'NOT OK'
    ;

Check whether there is a Redis server config correct,
now that the 'maxmemory-policy' setting is 'noeviction'.
Returns true if config correct and false otherwise.

It can be called as either the existing C<Redis::CappedCollection> object method or a class function.

If invoked as the object method, C<redis_config_ok> uses the C<redis>
attribute from the object as default.

If invoked as the class function, C<redis_config_ok> requires mandatory C<redis>
argument.

This argument are in key-value pair as described for L</create> method.

An error exception is thrown (C<confess>) if an argument is not valid.

=cut
sub redis_config_ok {
    return _maxmemory_policy_ok( @_ );
}

=head3 C<collection_info( redis =E<gt> $server, name =E<gt> $name )>

    my $info = $coll->collection_info;
    say 'An existing collection uses ', $info->{advance_cleanup_bytes}, " byte of 'advance_cleanup_bytes', ",
        $info->{items}, ' items are stored in ', $info->{lists}, ' lists';
    # or
    my $info = Redis::CappedCollection::collection_info(
        redis   => $redis,  # or redis => { server => "$server:$port" }
        name    => 'Collection name',
    );

Get collection information and status.
It can be called as either an existing C<Redis::CappedCollection> object method or a class function.

C<collection_info> arguments are in key-value pairs.
Arguments description match the arguments description for L</create> method:

=over 3

=item C<redis>

=item C<name>

=back

If invoked as the object method, C<collection_info>, arguments are optional and
use corresponding object attributes as defaults.

If called as a class methods, the arguments are mandatory.

Returns a reference to a hash with the following elements:

=over 3

=item *

C<lists> - Number of lists in a collection.

=item *

C<items> - Number of data items stored in the collection.

=item *

C<oldest_time> - Time of the oldest data in the collection.
C<undef> if the collection does not contain data.

=item *

C<older_allowed> - True if it is allowed to put data in collection that is older than the last element
removed from collection.

=item *

C<memory_reserve> - Memory reserve coefficient.

=item *

C<advance_cleanup_bytes> - The minimum size, in bytes, of the data to be released,
if the size of the collection data after adding new data may exceed C<'maxmemory'>.

=item *

C<advance_cleanup_num> - Maximum number of data elements to delete, if the size
of the collection data after adding new data may exceed C<'maxmemory'>.

=item *

C<data_version> - Data structure version.

=item *

C<last_removed_time> - time of the last removed element from collection
or 0 if nothing was removed from collection yet.

=back

An error will cause the program to throw an exception (C<confess>) if an argument is not valid
or the collection does not exist.

=cut
my @_collection_info_result_keys = qw(
    error
    lists
    items
    older_allowed
    advance_cleanup_bytes
    advance_cleanup_num
    memory_reserve
    data_version
    last_removed_time
    oldest_time
);

sub collection_info {
    my $results = {};
    my @ret;
    if ( @_ && _INSTANCE( $_[0], __PACKAGE__ ) ) {  # allow calling $obj->bar
        my $self   = shift;

        my %arguments = @_;
        _check_arguments_acceptability( \%arguments, [] );

        $self->_set_last_errorcode( $E_NO_ERROR );

        @ret = $self->_call_redis(
            $self->_lua_script_cmd( 'collection_info' ),
            0,
            $self->name,
        );
        $results = _lists2hash( \@_collection_info_result_keys, \@ret );

        my $error = $results->{error};

        if ( exists $ERROR{ $error } ) {
            $self->_clear_sha1 if $error == $E_COLLECTION_DELETED;
            $self->_throw( $error ) if $error != $E_NO_ERROR;
        } else {
            $self->_process_unknown_error( @ret );
        }
    } else {
        shift if _CLASSISA( $_[0], __PACKAGE__ );   # allow calling Foo->bar as well as Foo::bar

        my %arguments = @_;
        _check_arguments_acceptability( \%arguments, [ 'redis', 'name' ] );

        confess "'redis' argument is required"  unless defined $arguments{redis};
        confess "'name' argument is required"   unless defined $arguments{name};

        my $redis   = _get_redis( delete $arguments{redis} );
        my $name    = delete $arguments{name};

        confess( 'Unknown arguments: ', join( ', ', keys %arguments ) ) if %arguments;

        @ret = _call_redis(
            $redis,
            _lua_script_cmd( $redis, 'collection_info' ),
            0,
            $name,
        );
        $results = _lists2hash( \@_collection_info_result_keys, \@ret );

        my $error = $results->{error};

        if ( exists $ERROR{ $error } ) {
            _confess( "Collection '$name.' info not received (".$ERROR{ $error }.')' )
                if $error != $E_NO_ERROR;
        } else {
            _unknown_error( @ret );
        }
    }

    my $oldest_time = $results->{oldest_time};
    !$oldest_time || defined( _NUMBER( $oldest_time ) ) || warn( 'oldest_time is not a number: ', $oldest_time );

    delete $results->{error};
    return $results;
}

=head3 C<list_info( $list_id )>

Get data list information and status.

C<$list_id> must be a non-empty string.

Returns a reference to a hash with the following elements:

=over 3

=item *

C<items> - Number of data items stored in the data list.

=item *

C<oldest_time> - The time of the oldest data in the list.
C<undef> if the data list does not exist.

=back

=cut
my @_list_info_result_keys = qw(
    error
    items
    oldest_time
);

sub list_info {
    my ( $self, $list_id ) = @_;

    _STRING( $list_id ) // $self->_throw( $E_MISMATCH_ARG, 'list_id' );

    $self->_set_last_errorcode( $E_NO_ERROR );

    my @ret = $self->_call_redis(
        $self->_lua_script_cmd( 'list_info' ),
        0,
        $self->name,
        $list_id,
    );
    my $results = _lists2hash( \@_list_info_result_keys, \@ret );

    my $error = $results->{error};

    if ( exists $ERROR{ $error } ) {
        $self->_clear_sha1 if $error == $E_COLLECTION_DELETED;
        $self->_throw( $error ) if $error != $E_NO_ERROR;
    } else {
        $self->_process_unknown_error( @ret );
    }

    my $oldest_time = $results->{oldest_time};
    !$oldest_time || defined( _NUMBER( $oldest_time ) ) || warn( 'oldest_time is not a number: ', $oldest_time );

    delete $results->{error};
    return $results;
}

=head3 C<oldest_time>

    my $oldest_time = $coll->oldest_time;

Get the time of the oldest data in the collection.
Returns C<undef> if the collection does not contain data.

An error exception is thrown (C<confess>) if the collection does not exist.

=cut
my @_oldest_time_result_keys = qw(
    error
    oldest_time
);

sub oldest_time {
    my $self   = shift;

    $self->_set_last_errorcode( $E_NO_ERROR );

    my @ret = $self->_call_redis(
        $self->_lua_script_cmd( 'oldest_time' ),
        0,
        $self->name,
    );
    my $results = _lists2hash( \@_oldest_time_result_keys, \@ret );

    my $error = $results->{error};

    if ( exists $ERROR{ $error } ) {
        $self->_clear_sha1 if $error == $E_COLLECTION_DELETED;
        $self->_throw( $error ) if $error != $E_NO_ERROR;
    } else {
        $self->_process_unknown_error( @ret );
    }

    my $oldest_time = $results->{oldest_time};
    !$oldest_time || defined( _NUMBER( $oldest_time ) ) || warn( 'oldest_time is not a number: ', $oldest_time );

    return $oldest_time;
}

=head3 C<list_exists( $list_id )>

    say "The collection has '$list_id' list" if $coll->list_exists( 'Some_id' );

Check whether there is a list in the collection with given
ID C<$list_id>.

Returns true if the list exists and false otherwise.

=cut
sub list_exists {
    my ( $self, $list_id ) = @_;

    _STRING( $list_id ) // $self->_throw( $E_MISMATCH_ARG, 'list_id' );

    $self->_set_last_errorcode( $E_NO_ERROR );

    return $self->_call_redis( 'EXISTS', $self->_data_list_key( $list_id ) );
}

=head3 C<collection_exists( redis =E<gt> $server, name =E<gt> $name )>

    say 'The collection ', $coll->name, ' exists' if $coll->collection_exists;
    my $redis = Redis->new( server => "$server:$port" );
    say "The collection 'Some name' exists"
        if Redis::CappedCollection::collection_exists( redis => $redis, name => 'Some name' );

Check whether there is a collection with given name.
Returns true if the collection exists and false otherwise.

It can be called as either the existing C<Redis::CappedCollection> object method or a class function.

If invoked as the object method, C<collection_exists> uses C<redis> and C<name>
attributes from the object as defaults.

If invoked as the class function, C<collection_exists> requires mandatory C<redis> and C<name>
arguments.

These arguments are in key-value pairs as described for L</create> method.

An error exception is thrown (C<confess>) if an argument is not valid.

=cut
sub collection_exists {
    my ( $self, $redis, $name );
    if ( @_ && _INSTANCE( $_[0], __PACKAGE__ ) ) {  # allow calling $obj->bar
        $self   = shift;
        $redis  = $self->_redis;
        $name   = $self->name;
    } else {
        shift if _CLASSISA( $_[0], __PACKAGE__ );   # allow calling Foo->bar as well as Foo::bar
    }

    my %arguments = @_;
    _check_arguments_acceptability( \%arguments, [ 'redis', 'name' ] );

    unless ( $self ) {
        confess "'redis' argument is required"  unless defined $arguments{redis};
        confess "'name' argument is required"   unless defined $arguments{name};
    }

    $redis  = _get_redis( $arguments{redis} ) unless $self;
    $name   = $arguments{name} if exists $arguments{name};

    if ( $self ) {
        return $self->_call_redis( 'EXISTS', _make_status_key( $name ) );
    } else {
        return _call_redis( $redis, 'EXISTS', _make_status_key( $name ) );
    }
}

=head3 C<lists( $pattern )>

    say "The collection has '$_' list" foreach $coll->lists;

Returns an array of list ID of lists stored in a collection.
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

Warning: consider C<lists> as a command that should only be used in production
environments with extreme care. Its performance is not optimal for large collections.
This command is intended for debugging and special operations.
Don't use C<lists> in your regular application code.

In addition, it may cause an exception (C<confess>) if
the collection contains a very large number of lists
(C<'Error while reading from Redis server'>).

=cut
sub lists {
    my $self        = shift;
    my $pattern     = shift // '*';

    _STRING( $pattern ) // $self->_throw( $E_MISMATCH_ARG, 'pattern' );

    $self->_set_last_errorcode( $E_NO_ERROR );

    my @keys;
    try {
        @keys = $self->_call_redis( 'KEYS', $self->_data_list_key( $pattern ) );
    } catch {
        my $error = $_;
        _confess( $error ) unless $self->last_errorcode == $E_REDIS_DID_NOT_RETURN_DATA;
    };

    return map { ( $_ =~ /:([^:]+)$/ )[0] } @keys;
}

=head3 C<resize( redis =E<gt> $server, name =E<gt> $name, ... )>

    $coll->resize( advance_cleanup_bytes => 100_000 );
    my $redis = Redis->new( server => "$server:$port" );
    Redis::CappedCollection::resize( redis => $redis, name => 'Some name', older_allowed => 1 );

Use the C<resize> to change the values of the parameters of the collection.
It can be called as either the existing C<Redis::CappedCollection> object method or a class function.

If invoked as the object method, C<resize> uses C<redis> and C<name> attributes
from the object as defaults.
If invoked as the class function, C<resize> requires mandatory C<redis> and C<name>
arguments.

These arguments are in key-value pairs as described for L</create> method.

It is possible to change the following parameters: C<older_allowed>, C<advance_cleanup_bytes>,
C<advance_cleanup_num>, C<memory_reserve>. One or more parameters are required.

Returns the number of completed changes.

An error exception is thrown (C<confess>) if an argument is not valid or the
collection does not exist.

=cut
sub resize {
    my ( $self, $redis, $name );
    if ( @_ && _INSTANCE( $_[0], __PACKAGE__ ) ) {  # allow calling $obj->bar
        $self   = shift;
        $redis  = $self->_redis;
        $name   = $self->name;
    } else {
        shift if _CLASSISA( $_[0], __PACKAGE__ );   # allow calling Foo->bar as well as Foo::bar
    }

    my %arguments = @_;
    _check_arguments_acceptability( \%arguments, [ 'redis', 'name', @_status_parameters ] );

    unless ( $self ) {
        confess "'redis' argument is required"  unless defined $arguments{redis};
        confess "'name' argument is required"   unless defined $arguments{name};
    }

    $redis  = _get_redis( $arguments{redis} ) unless $self;
    $name   = $arguments{name} if $arguments{name};

    my $requested_changes = 0;
    foreach my $parameter ( @_status_parameters ) {
        ++$requested_changes if exists $arguments{ $parameter };
    }
    unless ( $requested_changes ) {
        my $error = 'One or more parameters are required';
        if ( $self ) {
            $self->_throw( $E_MISMATCH_ARG, $error );
        } else {
            confess "$error : ".$ERROR{ $E_MISMATCH_ARG }.')'
        }
    }

    my $resized = 0;
    foreach my $parameter ( @_status_parameters ) {
        if ( exists $arguments{ $parameter } ) {
            if ( $parameter eq 'advance_cleanup_bytes' || $parameter eq 'advance_cleanup_num' ) {
                confess "'$parameter' must be nonnegative integer"
                    unless _NONNEGINT( $arguments{ $parameter } );
            } elsif ( $parameter eq 'memory_reserve' ) {
                my $memory_reserve = $arguments{ $parameter };
                confess "'$parameter' must have a valid value"
                    unless _NUMBER( $memory_reserve ) && $memory_reserve >= $MIN_MEMORY_RESERVE && $memory_reserve <= $MAX_MEMORY_RESERVE;
            } elsif ( $parameter eq 'older_allowed' ) {
                $arguments{ $parameter } = $arguments{ $parameter } ? 1 :0;
            }

            my $ret = 0;
            my $new_val = $arguments{ $parameter };
            if ( $self ) {
                $ret = $self->_call_redis( 'HSET', _make_status_key( $self->name ), $parameter, $new_val );
            } else {
                $ret = _call_redis( $redis, 'HSET', _make_status_key( $name ), $parameter, $new_val );
            }

            if ( $ret == 0 ) {   # 0 if field already exists in the hash and the value was updated
                if ( $self ) {
                    if ( $parameter eq 'advance_cleanup_bytes' ) {
                        $self->_set_advance_cleanup_bytes( $new_val );
                    } elsif ( $parameter eq 'advance_cleanup_num' ) {
                        $self->_set_advance_cleanup_num( $new_val );
                    } elsif ( $parameter eq 'memory_reserve' ) {
                        $self->_set_memory_reserve( $new_val );
                    } else {
                        $self->$parameter( $new_val );
                    }
                }
                ++$resized;
            } else {
                my $msg = "Parameter $parameter not updated to '$new_val' for collection '$name'";
                if ( $self ) {
                    $self->_throw( $E_COLLECTION_DELETED, $msg );
                } else {
                    _confess( "$msg (".$ERROR{ $E_COLLECTION_DELETED }.')' );
                }
            }
        }
    }

    return $resized;
}

=head3 C<drop_collection( redis =E<gt> $server, name =E<gt> $name )>

    $coll->drop_collection;
    my $redis = Redis->new( server => "$server:$port" );
    Redis::CappedCollection::drop_collection( redis => $redis, name => 'Some name' );

Use the C<drop_collection> to remove the entire collection from the redis server,
including all its data and metadata.

Before using this method, make sure that the collection is not being used by other customers.

It can be called as either the existing C<Redis::CappedCollection> object method or a class function.
If invoked as the class function, C<drop_collection> requires mandatory C<redis> and C<name>
arguments.
These arguments are in key-value pairs as described for L</create> method.

Warning: consider C<drop_collection> as a command that should only be used in production
environments with extreme care. Its performance is not optimal for large collections.
This command is intended for debugging and special operations.
Avoid using C<drop_collection> in your regular application code.

C<drop_collection> mat throw an exception (C<confess>) if
the collection contains a very large number of lists
(C<'Error while reading from Redis server'>).

An error exception is thrown (C<confess>) if an argument is not valid.

=cut
sub drop_collection {
    my $ret;
    if ( @_ && _INSTANCE( $_[0], __PACKAGE__ ) ) {  # allow calling $obj->bar
        my $self = shift;

        my %arguments = @_;
        _check_arguments_acceptability( \%arguments, [] );

        $self->_set_last_errorcode( $E_NO_ERROR );

        $ret = $self->_call_redis(
            $self->_lua_script_cmd( 'drop_collection' ),
            0,
            $self->name,
        );

        $self->_clear_name;
        $self->_clear_sha1;
    } else {
        shift if _CLASSISA( $_[0], __PACKAGE__ );   # allow calling Foo->bar as well as Foo::bar

        my %arguments = @_;
        _check_arguments_acceptability( \%arguments, [ 'redis', 'name' ] );

        confess "'redis' argument is required"  unless defined $arguments{redis};
        confess "'name' argument is required"   unless defined $arguments{name};

        my $redis   = _get_redis( $arguments{redis} );
        my $name    = $arguments{name};

        $ret = _call_redis(
            $redis,
            _lua_script_cmd( $redis, 'drop_collection' ),
            0,
            $name,
        );
    }

    return $ret;
}

=head3 C<drop_list( $list_id )>

Use the C<drop> method to remove the entire specified list.
Method removes all the structures on the Redis server associated with
the specified list.

C<$list_id> must be a non-empty string.

Method returns true if the list is removed, or false otherwise.

=cut
my @_drop_list_result_keys = qw(
    error
    list_removed
);

sub drop_list {
    my ( $self, $list_id ) = @_;

    _STRING( $list_id ) // $self->_throw( $E_MISMATCH_ARG, 'list_id' );

    $self->_set_last_errorcode( $E_NO_ERROR );

    my @ret = $self->_call_redis(
        $self->_lua_script_cmd( 'drop_list' ),
        0,
        $self->name,
        $list_id,
    );
    my $results = _lists2hash( \@_drop_list_result_keys, \@ret );

    my $error = $results->{error};

    if ( exists $ERROR{ $error } ) {
        $self->_clear_sha1 if $error == $E_COLLECTION_DELETED;
        $self->_throw( $error ) if $error != $E_NO_ERROR;
    } else {
        $self->_process_unknown_error( @ret );
    }

    return $results->{list_removed};
}

=head3 C<clear_collection()>

    $coll->clear_collection;

Use the C<clear_collection> to remove the entire collection data from the redis server,

Before using this method, make sure that the collection is not being used by other customers.

Warning: consider C<clear_collection> as a command that should only be used in production
environments with extreme care. Its performance is not optimal for large collections.
This command is intended for debugging and special operations.
Avoid using C<clear_collection> in your regular application code.

C<clear_collection> mat throw an exception (C<confess>) if
the collection contains a very large number of lists
(C<'Error while reading from Redis server'>).

=cut
sub clear_collection {
    my $self = shift;

    my $ret;

    $self->_set_last_errorcode( $E_NO_ERROR );

    $ret = $self->_call_redis(
        $self->_lua_script_cmd( 'clear_collection' ),
        0,
        $self->name,
    );

    return $ret;
}

=head3 C<ping>

    $is_alive = $coll->ping;

This command is used to test if a connection is still alive.

Returns 1 if a connection is still alive or 0 otherwise.

External connections to the server object (eg, C <$redis = Redis->new( ... );>),
and the collection object can continue to work after calling ping only if the method returned 1.

If there is no connection to the Redis server (methods return 0), the connection to the server closes.
In this case, to continue working with the collection,
you must re-create the C<Redis::CappedCollection> object with the L</open> method.
When using an external connection to the server,
to check the connection to the server you can use the C<$redis->echo( ... )> call.
This is useful to avoid closing the connection to the Redis server unintentionally.

=cut
sub ping {
    my ( $self ) = @_;

    $self->_set_last_errorcode( $E_NO_ERROR );

    my $ret;
    try {
        $ret = $self->_redis->ping;
    } catch {
        $self->_redis_exception( $_ );
    };

    return( ( $ret // '<undef>' ) eq 'PONG' ? 1 : 0 );
}

=head3 C<quit>

    $coll->quit;

Close the connection with the redis server.

It does not close the connection to the Redis server if it is an external connection provided
to collection constructor as existing L<Redis> object.
When using an external connection (eg, C<$redis = Redis-> new (...);>),
to close the connection to the Redis server, call C<$redis->quit> after calling this method.

=cut
sub quit {
    my ( $self ) = @_;

    return if $] >= 5.14 && ${^GLOBAL_PHASE} eq 'DESTRUCT';

    $self->_set_last_errorcode( $E_NO_ERROR );
    $self->_clear_sha1;
    unless ( $self->_use_external_connection ) {
        try {
            $self->_redis->quit;
        } catch {
            $self->_redis_exception( $_ );
        };
    }

    return;
}

#-- private attributes ---------------------------------------------------------

has _DEBUG      => (
    is          => 'rw',
    init_arg    => undef,
    isa         => 'Num',
    default     => 0,
);

has _check_maxmemory        => (
    is          => 'ro',
    init_arg    => 'check_maxmemory',
    isa         => 'Bool',
    default     => 1,
);

has _create_from_naked_new  => (
    is          => 'ro',
    isa         => 'Bool',
    default     => 1,
);

has _create_from_open       => (
    is          => 'ro',
    isa         => 'Bool',
    default     => 0,
);

has _use_external_connection    => (
    is          => 'rw',
    isa         => 'Bool',
    default     => 1,
);

has _server                 => (
    is          => 'rw',
    isa         => 'Str',
    default     => $DEFAULT_SERVER.':'.$DEFAULT_PORT,
    trigger     => sub {
        my $self = shift;
        $self->_server( $self->_server.':'.$DEFAULT_PORT )
            unless $self->_server =~ /:/;
    },
);

has _redis                  => (
    is          => 'rw',
    # 'Maybe[Test::RedisServer]' to test only
    isa         => 'Maybe[Redis] | Maybe[Test::RedisServer]',
);

has _maxmemory              => (
    is          => 'rw',
    isa         => __PACKAGE__.'::NonNegInt',
    init_arg    => undef,
);

foreach my $attr_name ( qw(
        _queue_key
        _status_key
        _data_keys
        _time_keys
    ) ) {
    has $attr_name          => (
        is          => 'rw',
        isa         => 'Str',
        init_arg    => undef,
    );
}

my $_lua_scripts = {};

#-- private functions ----------------------------------------------------------

sub _check_arguments_acceptability {
    my ( $received_arguments, $acceptable_arguments ) = @_;

    my ( %legal_arguments, @unlegal_arguments );
    $legal_arguments{ $_ } = 1 foreach @$acceptable_arguments;
    foreach my $argument ( keys %$received_arguments ) {
        push @unlegal_arguments, $argument unless exists $legal_arguments{ $argument };
    }

    confess( 'Unknown arguments: ', join( ', ', @unlegal_arguments ) ) if @unlegal_arguments;

    return;
}

sub _maxmemory_policy_ok {
    my ( $self, $redis );
    if ( @_ && _INSTANCE( $_[0], __PACKAGE__ ) ) {  # allow calling $obj->bar
        $self   = shift;
        $redis  = $self->_redis;
    } else {
        shift if _CLASSISA( $_[0], __PACKAGE__ );   # allow calling Foo->bar as well as Foo::bar
    }

    my %arguments = @_;
    _check_arguments_acceptability( \%arguments, [ 'redis' ] );

    my $maxmemory_policy;
    if ( $self ) {
        ( undef, $maxmemory_policy ) = $self->_call_redis( 'CONFIG', 'GET', 'maxmemory-policy' );
    } else {
        my $redis_argument = $arguments{redis};
        confess "'redis' argument is required" unless defined( $redis_argument );
        ( undef, $maxmemory_policy ) = _call_redis( _get_redis( $redis_argument ), 'CONFIG', 'GET', 'maxmemory-policy' )
    }

    return( defined( $maxmemory_policy ) && $maxmemory_policy eq $USED_MEMORY_POLICY );
}

sub _lists2hash {
    my ( $keys, $vals ) = @_;

    confess $ERROR{ $E_MISMATCH_ARG }." for internal function '_lists2hash'"
        unless _ARRAY( $keys ) && _ARRAY0( $vals ) && scalar( @$keys ) >= scalar( @$vals );

    my %hash;
    for ( my $idx = 0; $idx < @$keys; $idx++ ) {
        $hash{ $keys->[ $idx ] } = $vals->[ $idx ];
    }

    return \%hash;
}

sub _process_unknown_error {
    my ( $self, @args ) = @_;

    $self->_set_last_errorcode( $E_UNKNOWN_ERROR );
    _unknown_error( @args, $self->reconnect_on_error ? $self->_reconnect( $E_UNKNOWN_ERROR ) : () );
}

sub _unknown_error {
    my @args = @_;

    _confess( $ERROR{ $E_UNKNOWN_ERROR }, ': ', join( ', ', map { $_ // '<undef>' } @args ) );
}

sub _confess {
    my @args = @_;

    confess @args;
}

sub _make_data_key {
    my ( $name ) = @_;
    return( $NAMESPACE.':D:'.$name );
}

sub _make_time_key {
    my ( $name ) = @_;
    return( $NAMESPACE.':T:'.$name );
}

sub _make_status_key {
    my ( $name ) = @_;
    return( $NAMESPACE.':S:'.$name );
}

sub _get_redis {
    my ( $redis ) = @_;

    $redis = _redis_constructor( $redis )
        unless _INSTANCE( $redis, 'Redis' );

    return $redis;
}

#-- private methods ------------------------------------------------------------

# for testing only
sub _long_term_operation {
    my ( $self, $return_as_insert ) = @_;

    $self->_set_last_errorcode( $E_NO_ERROR );

    my @ret = $self->_call_redis(
        $self->_lua_script_cmd( '_long_term_operation' ),
        0,
        $self->name,
        $return_as_insert ? 1 : 0,
    );

    my ( $error ) = @ret;

    if ( $return_as_insert ) {
        if ( scalar( @ret ) == 1 && exists( $ERROR{ $error } ) ) {
            if ( $error == $E_NO_ERROR ) {
                # Normal result: Nothing to do
            } elsif ( $error == $E_COLLECTION_DELETED ) {
                $self->_clear_sha1;
                $self->_throw( $error );
            } elsif (
                       $error == $E_DATA_ID_EXISTS
                    || $error == $E_OLDER_THAN_ALLOWED
                ) {
                $self->_throw( $error );
            } else {
                $self->_throw( $error, 'Unexpected error' );
            }
        } else {
            $self->_process_unknown_error( @ret );
        }
    } else {
        if ( scalar( @ret ) == 3 && exists( $ERROR{ $error } ) && $ret[2] eq '_long_term_operation' ) {
            if ( $error == $E_NO_ERROR ) {
                # Normal result: Nothing to do
            } else {
                $self->_throw( $error, 'Unexpected error' );
            }
        } else {
            $self->_process_unknown_error( @ret );
        }
    }

    return \@ret;
}

sub _data_list_key {
    my ( $self, $list_id ) = @_;

    return( $self->_data_keys.':'.$list_id );
}

sub _time_list_key {
    my ( $self, $list_id ) = @_;

    return( $self->_time_keys.':'.$list_id );
}

sub _verify_collection {
    my ( $self ) = @_;

    $self->_set_last_errorcode( $E_NO_ERROR );

    my ( $status_exist, $older_allowed, $advance_cleanup_bytes, $advance_cleanup_num, $memory_reserve, $data_version ) = $self->_call_redis(
        $self->_lua_script_cmd( 'verify_collection' ),
        0,
        $self->name,
        $self->older_allowed ? 1 : 0,
        $self->advance_cleanup_bytes || 0,
        $self->advance_cleanup_num || 0,
        $self->memory_reserve || $MIN_MEMORY_RESERVE,
        );

    if ( $status_exist ) {
        $self->advance_cleanup_bytes( $advance_cleanup_bytes )  unless $self->advance_cleanup_bytes;
        $self->advance_cleanup_num( $advance_cleanup_num )      unless $self->advance_cleanup_num;
        $older_allowed          == $self->older_allowed         or $self->_throw( $E_MISMATCH_ARG, 'older_allowed' );
        $advance_cleanup_bytes  == $self->advance_cleanup_bytes or $self->_throw( $E_MISMATCH_ARG, 'advance_cleanup_bytes' );
        $advance_cleanup_num    == $self->advance_cleanup_num   or $self->_throw( $E_MISMATCH_ARG, 'advance_cleanup_num' );
        $memory_reserve         == $self->memory_reserve        or $self->_throw( $E_MISMATCH_ARG, 'memory_reserve' );
        $data_version           == $DATA_VERSION                or $self->_throw( $E_INCOMP_DATA_VERSION );
    }
}

sub _reconnect {
    my $self    = shift;
    my $err     = shift // 0;
    my $msg     = shift;

    my $err_msg = '';
    if (
            !$err || (
                   $err != $E_MISMATCH_ARG
                && $err != $E_DATA_TOO_LARGE
                && $err != $E_MAXMEMORY_LIMIT
                && $err != $E_MAXMEMORY_POLICY
            )
        ) {
        try {
            $self->_redis->connect;
        } catch {
            my $error = $_;
            $err_msg = "(Not reconnected: $error)";
        };
    }

    if ( $err_msg ) {
        $msg = defined( $msg )
            ? ( $msg ? "$msg " : '' )."($err_msg)"
            : $err_msg;
    }

    return $msg;
}

sub _throw {
    my ( $self, $err, $prefix ) = @_;

    $prefix = $self->_reconnect( $err, $prefix ) if $self->reconnect_on_error;

    if ( exists $ERROR{ $err } ) {
        $self->_set_last_errorcode( $err );
        _confess( ( $prefix ? "$prefix : " : '' ).$ERROR{ $err } );
    } else {
        $self->_set_last_errorcode( $E_UNKNOWN_ERROR );
        _confess $ERROR{ $E_UNKNOWN_ERROR }, ': ', ( $prefix ? "$prefix : " : '' ), $err // '<undef>';
    }
}

{
    my ( $_running_script_name, $_running_script_body );

    sub _lua_script_cmd {
        my ( $self, $redis );
        if ( _INSTANCE( $_[0], __PACKAGE__ ) ) {    # allow calling $obj->bar
            $self   = shift;
            $redis  = $self->_redis;
        } else {                                    # allow calling Foo::bar
            $redis  = shift;
        }

        $_running_script_name = shift;
        $_running_script_body = $lua_script_body{ $_running_script_name };

        my $sha1 = $_lua_scripts->{ $redis }->{ $_running_script_name };
        unless ( $sha1 ) {
            $sha1 = $_lua_scripts->{ $redis }->{ $_running_script_name } = sha1_hex( $_running_script_body );
            my $ret;
            if ( $self ) {
                $ret = ( $self->_call_redis( 'SCRIPT', 'EXISTS', $sha1 ) )[0];
            } else {
                $ret = ( _call_redis( $redis, 'SCRIPT', 'EXISTS', $sha1 ) )[0];
            }
            return( 'EVAL', $_running_script_body )
                unless $ret;
        }
        return( 'EVALSHA', $sha1 );
    }

    sub _redis_exception {
        my $self;
        $self = shift if _INSTANCE( $_[0], __PACKAGE__ );   # allow calling $obj->bar
        my ( $error ) = @_;                                 # allow calling Foo::bar

        my $err_msg = '';
        if ( $self ) {
            # Use the error messages from Redis.pm
            if (
                       $error =~ /^Could not connect to Redis server at /
                    || $error =~ /^Can't close socket: /
                    || $error =~ /^Not connected to any server/
                    # Maybe for pub/sub only
                    || $error =~ /^Error while reading from Redis server: /
                    || $error =~ /^Redis server closed connection/
                ) {
                $self->_set_last_errorcode( $E_NETWORK );

                # For connection problem
                $err_msg = $self->_reconnect( $E_UNKNOWN_ERROR, $err_msg ) if $self->reconnect_on_error;
            } elsif (
                       $error =~ /^\[[^]]+\]\s+-?\Q$REDIS_MEMORY_ERROR_MSG\E/i
                    || $error =~ /^\[[^]]+\]\s+-?\Q$REDIS_ERROR_CODE $ERROR{ $E_MAXMEMORY_LIMIT }\E/i
                    || $error =~ /^\[[^]]+\]\s+-NOSCRIPT No matching script. Please use EVAL./
                ) {
                $self->_set_last_errorcode( $E_MAXMEMORY_LIMIT );
                $self->_clear_sha1;

                # No connection problem
            } elsif ( $error =~ /^\[[^]]+\]\s+BUSY Redis is busy running a script/ ){
                $self->_set_last_errorcode( $E_UNKNOWN_ERROR );

                # No connection problem - must wait...
            } else {    # external ALRM processing here
                $self->_set_last_errorcode( $E_REDIS );

                # For possible connection problems
                $err_msg = $self->_reconnect( $E_UNKNOWN_ERROR, $err_msg ) if $self->reconnect_on_error;
            }
        } else {
            # nothing to do now
        }

        if ( $error =~ /\] ERR Error (?:running|compiling) script/ ) {
            $error .= "\nLua script '$_running_script_name':\n$_running_script_body";
        }
        _confess( $error, $err_msg || () );
    }
}

sub _clear_sha1 {
    my ( $self ) = @_;

    delete( $_lua_scripts->{ $self->_redis } ) if $self->_redis;
}

sub _redis_constructor {
    my ( $self, $redis, $redis_parameters );
    if ( @_ && _INSTANCE( $_[0], __PACKAGE__ ) ) {  # allow calling $obj->bar
        $self               = shift;
        $redis_parameters   = shift;

        if ( $redis_parameters ) {
            $self->_set_last_errorcode( $E_NO_ERROR );
            $redis = try {
                Redis->new( %$redis_parameters );
            } catch {
                $self->_redis_exception( $_ );
            };
        } else {
            $redis = $self->_redis;
        }
    } else {                                        # allow calling Foo::bar
        $redis_parameters = shift;
        $redis = try {
            Redis->new( %$redis_parameters );
        } catch {
            confess "'Redis' exception: $_";
        };
    }

    return $redis;
}

# Keep in mind the default 'redis.conf' values:
# Close the connection after a client is idle for N seconds (0 to disable)
#    timeout 300

# Send a request to Redis
sub _call_redis {
    my ( $self, $redis );
    if ( _INSTANCE( $_[0], __PACKAGE__ ) ) {    # allow calling $obj->bar
        $self = shift;

        if ( $self->reconnect_on_error && !$self->ping ) {
            my $err_msg = $self->_reconnect();
            _confess( $ERROR{$E_REDIS}, $err_msg ) if $err_msg;
        }

        $redis = $self->_redis;
    } else {                                    # allow calling Foo::bar
        $redis  = shift;
    }
    my $method  = shift;

    $self->_set_last_errorcode( $E_NO_ERROR ) if $self;

    my @return;
    my @args = @_;
    try {
        @return = $redis->$method( map { ref( $_ ) ? $$_ : $_ } @args );
    } catch {
        my $error = $_;
        if ( $self ) {
            $self->_redis_exception( $error );
        } else {
            _redis_exception( $error );
        }
    };

    unless ( scalar @return ) {
        $self->_set_last_errorcode( $E_REDIS_DID_NOT_RETURN_DATA )
            if $self;
        confess $ERROR{ $E_REDIS_DID_NOT_RETURN_DATA };
    }

    return wantarray ? @return : $return[0];
}

sub DESTROY {
    my ( $self ) = @_;

    $self->clear_sha1;
}

#-- Closes and cleans up -------------------------------------------------------

no Mouse::Util::TypeConstraints;
no Mouse;                                       # keywords are removed from the package
__PACKAGE__->meta->make_immutable();

=head2 DIAGNOSTICS

All recognizable errors in C<Redis::CappedCollection> set corresponding value
into the L</last_errorcode> and throw an exception (C<confess>).
Unidentified errors also throw exceptions but L</last_errorcode> is not set.

In addition to errors in the L<Redis|Redis> module, detected errors are
L</$E_MISMATCH_ARG>, L</$E_DATA_TOO_LARGE>, L</$E_MAXMEMORY_POLICY>, L</$E_COLLECTION_DELETED>,
L</$E_DATA_ID_EXISTS>, L</$E_OLDER_THAN_ALLOWED>, L</$E_NONEXISTENT_DATA_ID>,
L</$E_INCOMP_DATA_VERSION>, L</$E_REDIS_DID_NOT_RETURN_DATA>, L</$E_UNKNOWN_ERROR>.

The user has the choice:

=over 3

=item *

Use the module methods and independently analyze the situation without the use
of L</last_errorcode>.

=item *

Piece of code wrapped in C<eval {...};> and analyze L</last_errorcode>
(look at the L</"An Example"> section).

=back

=head2 An Example

An example of error handling.

    use 5.010;
    use strict;
    use warnings;

    #-- Common ---------------------------------------------------------
    use Redis::CappedCollection qw(
        $DEFAULT_SERVER
        $DEFAULT_PORT

        $E_NO_ERROR
        $E_MISMATCH_ARG
        $E_DATA_TOO_LARGE
        $E_NETWORK
        $E_MAXMEMORY_LIMIT
        $E_MAXMEMORY_POLICY
        $E_COLLECTION_DELETED
        $E_REDIS
    );

    # Error handling
    sub exception {
        my $coll    = shift;
        my $err     = shift;

        die $err unless $coll;
        if ( $coll->last_errorcode == $E_NO_ERROR ) {
            # For example, to ignore
            return unless $err;
        } elsif ( $coll->last_errorcode == $E_MISMATCH_ARG ) {
            # Necessary to correct the code
        } elsif ( $coll->last_errorcode == $E_DATA_TOO_LARGE ) {
            # Limit data length
        } elsif ( $coll->last_errorcode == $E_NETWORK ) {
            # For example, sleep
            #sleep 60;
            # and return code to repeat the operation
            #return 'to repeat';
        } elsif ( $coll->last_errorcode == $E_MAXMEMORY_LIMIT ) {
            # For example, return code to restart the server
            #return 'to restart the redis server';
        } elsif ( $coll->last_errorcode == $E_MAXMEMORY_POLICY ) {
            # Correct Redis server 'maxmemory-policy' setting
        } elsif ( $coll->last_errorcode == $E_COLLECTION_DELETED ) {
            # For example, return code to ignore
            #return "to ignore $err";
        } elsif ( $coll->last_errorcode == $E_REDIS ) {
            # Independently analyze the $err
        } elsif ( $coll->last_errorcode == $E_DATA_ID_EXISTS ) {
            # For example, return code to reinsert the data
            #return "to reinsert with new data ID";
        } elsif ( $coll->last_errorcode == $E_OLDER_THAN_ALLOWED ) {
            # Independently analyze the situation
        } else {
            # Unknown error code
        }
        die $err if $err;
    }

    my ( $list_id, $coll, @data );

    eval {
        $coll = Redis::CappedCollection->create(
            redis   => $DEFAULT_SERVER.':'.$DEFAULT_PORT,
            name    => 'Some name',
        );
    };
    exception( $coll, $@ ) if $@;
    say "'", $coll->name, "' collection created.";

    #-- Producer -------------------------------------------------------
    #-- New data

    eval {
        $list_id = $coll->insert(
            'Some List_id', # list id
            123,            # data id
            'Some data',
        );
        say "Added data in a list with '", $list_id, "' id" );

        # Change the "zero" element of the list with the ID $list_id
        if ( $coll->update( $list_id, 0, 'New data' ) ) {
            say 'Data updated successfully';
        } else {
            say 'Failed to update element';
        }
    };
    exception( $coll, $@ ) if $@;

    #-- Consumer -------------------------------------------------------
    #-- Fetching the data

    eval {
        @data = $coll->receive( $list_id );
        say "List '$list_id' has '$_'" foreach @data;
        # or to obtain records in the order they were placed
        while ( my ( $list_id, $data ) = $coll->pop_oldest ) {
            say "List '$list_id' had '$data'";
        }
    };
    exception( $coll, $@ ) if $@;

    #-- Utility --------------------------------------------------------
    #-- Getting statistics

    my ( $lists, $items );
    eval {
        my $info = $coll->collection_info;
        say 'An existing collection uses ', $info->{advance_cleanup_bytes}, " byte of 'advance_cleanup_bytes', ",
            'in ', $info->{items}, ' items are placed in ',
            $info->{lists}, ' lists';

        say "The collection has '$list_id' list"
            if $coll->list_exists( 'Some_id' );
    };
    exception( $coll, $@ ) if $@;

    #-- Closes and cleans up -------------------------------------------

    eval {
        $coll->quit;

        # Before use, make sure that the collection
        # is not being used by other clients
        #$coll->drop_collection;
    };
    exception( $coll, $@ ) if $@;

=head2 CappedCollection data structure

Using currently selected database (default = 0).

CappedCollection package creates the following data structures on Redis:

    #-- To store collection status:
    # HASH    Namespace:S:Collection_id
    # For example:
    $ redis-cli
    redis 127.0.0.1:6379> KEYS C:S:*
    1) "C:S:Some collection name"
    #   | |                  |
    #   | +-------+          +------------+
    #   |         |                       |
    #  Namespace  |                       |
    #  Fixed symbol of a properties hash  |
    #                         Capped Collection id
    ...
    redis 127.0.0.1:6379> HGETALL "C:S:Some collection name"
    1) "lists"                  # hash key
    2) "1"                      # the key value
    3) "items"                  # hash key
    4) "1"                      # the key value
    5) "older_allowed"          # hash key
    6) "0"                      # the key value
    7) "advance_cleanup_bytes"  # hash key
    8) "0"                      # the key value
    9) "advance_cleanup_num"    # hash key
    10) "0"                     # the key value
    11) "memory_reserve"        # hash key
    12) "0,05"                  # the key value
    13) "data_version"          # hash key
    14) "3"                     # the key value
    15) "last_removed_time"     # hash key
    16) "0"                     # the key value

    #-- To store collection queue:
    # ZSET    Namespace:Q:Collection_id
    # For example:
    redis 127.0.0.1:6379> KEYS C:Q:*
    1) "C:Q:Some collection name"
    #   | |                  |
    #   | +------+           +-----------+
    #   |        |                       |
    #  Namespace |                       |
    #  Fixed symbol of a queue           |
    #                        Capped Collection id
    ...
    redis 127.0.0.1:6379> ZRANGE "C:Q:Some collection name" 0 -1 WITHSCORES
    1) "Some list id" ----------+
    2) "1348252575.6651001"     |
    #           |               |
    #  Score: oldest data_time  |
    #                   Member: Data List id
    ...

    #-- To store CappedCollection data:
    # HASH    Namespace:I:Collection_id:DataList_id
    # HASH    Namespace:D:Collection_id:DataList_id
    # If the amount of data in the list is greater than 1
    # ZSET    Namespace:T:Collection_id:DataList_id
    # For example:
    redis 127.0.0.1:6379> KEYS C:[DT]:*
    1) "C:D:Some collection name:Some list id"
    # If the amount of data in the list is greater than 1
    2) "C:T:Some collection name:Some list id"
    #   | |                  |             |
    #   | +-----+            +-------+     + ---------+
    #   |       |                    |                |
    # Namespace |                    |                |
    # Fixed symbol of a list of data |                |
    #                    Capped Collection id         |
    #                                         Data list id
    ...
    redis 127.0.0.1:6379> HGETALL "C:D:Some collection name:Some list id"
    1) "0"                      # hash key: Data id
    2) "Some stuff"             # the key value: Data
    ...
    # If the amount of data in the list is greater than 1
    redis 127.0.0.1:6379> ZRANGE "C:T:Some collection name:Some list id" 0 -1 WITHSCORES
    1) "0" ---------------+
    2) "1348252575.5906"  |
    #           |         |
    #   Score: data_time  |
    #              Member: Data id
    ...

=head1 DEPENDENCIES

In order to install and use this package Perl version 5.010 or better is
required. Redis::CappedCollection module depends on other packages
that are distributed separately from Perl. We recommend the following packages
to be installed before installing Redis::CappedCollection :

    Const::Fast
    Digest::SHA1
    Mouse
    Params::Util
    Redis
    Try::Tiny

The Redis::CappedCollection module has the following optional dependencies:

    Data::UUID
    JSON::XS
    Net::EmptyPort
    Test::Exception
    Test::NoWarnings
    Test::RedisServer

If the optional modules are missing, some "prereq" tests are skipped.

The installation of the missing dependencies can either be accomplished
through your OS package manager or through CPAN (or downloading the source
for all dependencies and compiling them manually).

=head1 BUGS AND LIMITATIONS

Redis server version 2.8 or higher is required.

The use of C<maxmemory-police all*> in the F<redis.conf> file could lead to
a serious (and hard to detect) problem as Redis server may delete
the collection element. Therefore the C<Redis::CappedCollection> does not work with
mode C<maxmemory-police all*> in the F<redis.conf>.

It may not be possible to use this module with the cluster of Redis servers
because full name of some Redis keys may not be known at the time of the call
the Redis Lua script (C<'EVAL'> or C<'EVALSHA'> command).
So the Redis server may not be able to correctly forward the request
to the appropriate node in the cluster.

We strongly recommend setting C<maxmemory> option in the F<redis.conf> file.

Old data with the same time will be forced out in no specific order.

The collection API does not support deleting a single data item.

UTF-8 data should be serialized before passing to C<Redis::CappedCollection> for storing in Redis.

According to L<Redis|Redis> documentation:

=over 3

=item *

This module consider that any data sent to the Redis server is a raw octets string,
even if it has utf8 flag set.
And it doesn't do anything when getting data from the Redis server.

TODO: implement tests for

=over 3

=item *

memory errors (working with internal ROLLBACK commands)

=item *

working when maxmemory = 0 (in the F<redis.conf> file)

=back

WARNING: According to C<initServer()> function in F<redis.c> :

    /* 32 bit instances are limited to 4GB of address space, so if there is
     * no explicit limit in the user provided configuration we set a limit
     * at 3 GB using maxmemory with 'noeviction' policy'. This avoids
     * useless crashes of the Redis instance for out of memory. */

The C<Redis::CappedCollection> module was written, tested, and found working
on recent Linux distributions.

There are no known bugs in this package.

Please report problems to the L</"AUTHOR">.

Patches are welcome.

=back

=head1 MORE DOCUMENTATION

All modules contain detailed information on the interfaces they provide.

=head1 SEE ALSO

The basic operation of the Redis::CappedCollection package module:

L<Redis::CappedCollection|Redis::CappedCollection> - Object interface to create
a collection, addition of data and data manipulation.

L<Redis|Redis> - Perl binding for Redis database.

=head1 SOURCE CODE

Redis::CappedCollection is hosted on GitHub:
L<https://github.com/TrackingSoft/Redis-CappedCollection>

=head1 AUTHOR

Sergey Gladkov, E<lt>sgladkov@trackingsoft.comE<gt>

=head1 CONTRIBUTORS

Alexander Solovey

Jeremy Jordan

Vlad Marchenko

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2012-2016 by TrackingSoft LLC.

This package is free software; you can redistribute it and/or modify it under
the same terms as Perl itself. See I<perlartistic> at
L<http://dev.perl.org/licenses/artistic.html>.

This program is
distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE.

=cut

__DATA__
