/* Database Schema for Energy Mmanagement Cloud
 *
 */

/* 
 * Basics 
 */
/* UUIDs */
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE SCHEMA IF NOT EXISTS nodes;
CREATE SEQUENCE nodes.tsn;

/* Table to manage nodes 
 * 
 * Table is fully replicated bewetween all nodes
 * 
 */
CREATE TABLE nodes.systems (
     nodeid     uuid,
     url        text,
     data       text,
     clockid    bigint,
     tsn        bigint,
     primary key (url, data),
     unique ( clockid, tsn )
);

CREATE OR REPLACE FUNCTION nodes.clockid() RETURNS bigint AS $$
   BEGIN
/* for the moment, we assume the clockid is 0 
 *
 * Generally speaking a clockid is unique to the clock, every tsn generator has to
 * its globally unique id number. For every clock instance it is a constant.
 */
      RETURN 0;
   END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION nodes.register( _url text, _data text) RETURNS UUID  AS $$
   DECLARE
      _uuid uuid;
   BEGIN
     _uuid := uuid_generate_v4();

     insert into nodes.systems( nodeid, url, data, clockid, tsn  )
     values ( _uuid, _url, _data, nodes.clockid(), nextval( 'nodes.tsn' ) );

     RETURN _uuid;
   END
$$ LANGUAGE plpgsql;


CREATE TABLE nodes.highwatermarks (
     nodeid     uuid  primary key,
     clockid    bigint,
     tsn        bigint,
     unique ( clockid, tsn )
)

CREATE OR REPLACE FUNCTION nodes.getHighs() RETURNS TABLE( _nodeid uuid, _clockid bigint, _tsn bigint ) AS $$
   BEGIN
     RETURN QUERY
        select nodeid, clockid, tsn from nodes.highwatermarks;
   END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION nodes.putHigh( _nodeid uuid, _clockid bigint, _tsn bigint) RETURNS VOID AS $$
   BEGIN
       LOOP
            UPDATE nodes.highwatermarks
              SET tsn = _tsn, clockid = _clockid
              where nodeid = _nodeid;
        IF found THEN
            RETURN;
        END IF;
        BEGIN
            INSERT INTO nodes.highwatermarks( nodeid, clockid, tsn ) 
            VALUES( _nodeid, _clockid, _tsn );
            
            EXCEPTION WHEN unique_violation THEN
            -- Do nothing, and loop to try the UPDATE again.
        END;   
       END LOOP;
   END
$$ LANGUAGE plpgsql;



/*
 * OLD
 */

/*
 * First Schema setup for power data management
 * 
 */


/* 
 * A basic schema for power data
 */

CREATE SCHEMA IF NOT EXISTS power2;


/* for playing around we have one table with a string as key and another string as value
 *
 * TSN and key are primary keys
 * value is text (to be replaced to JSON data or others
 *
 * There are always two tables, one is for the actual state and the other is for recording history
 * 
 * The basic assumption is that the TSN is unique in itself, we can use the same structure for both.
 * For the history data we will need a 'delete' indicator. The 'delete' indicator can be the NULL 
 * indicator of the text field (since having this being NULL doesn't mean anything else ). We can 
 * then keep both tables equal.
 */
CREATE TABLE power2.data (
    tsn bigint,
    key text unique,
    value text,
    primary key( tsn )
);

/* 
 * History table (the same as the other)
 */
CREATE TABLE power2.data_h ( like power2.data );

/* Functions for put and get
 *
 * put always creates new entry with new TSN
 * (most simple approach, can be improved)
 * 
 * get needs to select the latest entry per key (subselect needed)
 */

CREATE OR REPLACE FUNCTION power2.put( _key text, _value text) RETURNS VOID AS $$
   DECLARE
       _tsn bigint;
   BEGIN

     _tsn := nextval( 'clock.tsn');

     DELETE FROM power2.data where key = _key;
     INSERT INTO power2.data( tsn, key, value) 
     VALUES( _tsn, _key, _value ); 

     INSERT INTO power2.data_h( tsn, key, value) 
     VALUES( _tsn, _key, _value ); 

   END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION power2.get( _key text) 
     RETURNS TABLE( ret_tsn bigint, ret_key text, ret_value text ) AS $$
   BEGIN

     RETURN QUERY
       SELECT tsn, key, value FROM power2.data 
           WHERE key = _key LIMIT 1; 

   END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION power2.delete( _key text ) RETURNS VOID AS $$
   DECLARE
       _tsn bigint;
   BEGIN

     _tsn := nextval( 'clock.tsn');

     DELETE FROM power2.data where key = _key;

     INSERT INTO power2.data_h( tsn, key, value ) 
     VALUES( _tsn, _key, NULL ); 

   END;
$$ LANGUAGE plpgsql;

/*
 * we want to create a clock as a singleton. Since the clock is basically a sequence,
 * the global clock is defined as a schema containing a sequence as the base structure
 */

CREATE SCHEMA IF NOT EXISTS clock;
CREATE SEQUENCE clock.tsn;


CREATE OR REPLACE FUNCTION clock.new_tsn() RETURNS BIGINT AS $$
   BEGIN
    
     RETURN nextval( 'clock.tsn' );

   END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION clock.last_tsn() RETURNS BIGINT AS $$
   BEGIN
    
     RETURN currval( 'clock.tsn' );

   END;
$$ LANGUAGE plpgsql;

CREATE SCHEMA IF NOT EXISTS nodes;


CREATE TABLE nodes.highwatermarks (
     nodeid     bigint  primary key,
     hwm        bigint
);


CREATE OR REPLACE FUNCTION nodes.register( _url text, _data text) RETURNS UUID  AS $$
   DECLARE 
      _uuid uuid;
   BEGIN
     _uuid := uuid_generate_v4();

     insert into nodes.systems( nodeid, tsn, url, data ) 
     values ( _uuid, nextval( 'clock.tsn' ), _url, _data );

     RETURN _uuid;
   END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION nodes.update( _url text, _data text) RETURNS VOID AS $$
   DECLARE
       _tsn bigint;
   BEGIN

     _tsn := nextval( 'clock.tsn');

     LOOP
        -- first try to update the key
        UPDATE nodes.systems  SET tsn = _tsn, data = _data  WHERE url = _url;
        IF found THEN
            RETURN;
        END IF;
        -- not there, so try to insert the key
        -- if someone else inserts the same key concurrently,
        -- we could get a unique-key failure
        BEGIN
            INSERT INTO nodes.systems( tsn, url, data ) VALUES (  _tsn, _url, _data );
            RETURN;
        EXCEPTION WHEN unique_violation THEN
            -- do nothing, and loop to try the UPDATE again
        END;
    END LOOP;

   END;
$$ LANGUAGE plpgsql;

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

select uuid_generate_v4();

