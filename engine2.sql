/* Database Schema for Energy Mmanagement Cloud
 *
 */

/* 
 * Basics 
 */
/* UUIDs */
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

/* 
 * Schema to manage system nodes 
 */
CREATE SCHEMA IF NOT EXISTS nodes;

/* 
 * sequence counter (local) (manged via clockid)
 */
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

/* 
 * Information about local clock (nodes.tsn)
 *
 * single row table , key = 0
 */
CREATE TABLE nodes.clockid (
  clock   int primary key,
  nodeid  uuid,
  clockid bigint
);

/* 
 * Returns the id of the local 'clock'
 *
 * (creates the 'clock, if not existing (boot strap)
 */
CREATE OR REPLACE FUNCTION nodes.clockid() RETURNS bigint AS $$
   DECLARE 
      _clockid bigint;
   BEGIN
      LOOP
	  select clockid from nodes.clockid where clock = 0 into _clockid;
	  IF NOT FOUND THEN
/* to be improved: every instance must be different */
	       INSERT INTO nodes.clockid( clock, clockid ) 
		  VALUES ( 0, nextval( 'nodes.tsn' ) );
	  ELSE
	       RETURN _clockid;
	  END IF;
      END LOOP;

   END
$$ LANGUAGE plpgsql;

/*
 * Registers the local node (initial)
 */
CREATE OR REPLACE FUNCTION nodes.register( _url text, _data text) RETURNS UUID  AS $$
   DECLARE
      _uuid uuid;
   BEGIN
     _uuid := uuid_generate_v4();

     INSERT INTO nodes.clockid( clock, nodeid, clockid )
                  VALUES ( 0, _uuid, nextval( 'nodes.tsn' ) );

     insert into nodes.systems( nodeid, url, data, clockid, tsn  )
     values ( _uuid, _url, _data, nodes.clockid(), nextval( 'nodes.tsn' ) );


     RETURN _uuid;
   END
$$ LANGUAGE plpgsql;

/*
 * High-water mark vecor of all known nodes
 */
CREATE TABLE nodes.highwatermarks (
     nodeid     uuid  primary key,
     clockid    bigint,
     tsn        bigint,
     unique ( clockid, tsn )
);

/* 
 * read the (received) high-water marks of remote nodes
 */
CREATE OR REPLACE FUNCTION nodes.getRemoteHighs() RETURNS TABLE( _nodeid uuid, _clockid bigint, _tsn bigint ) AS $$
   BEGIN
     RETURN QUERY
        select nodeid, clockid, tsn from nodes.highwatermarks;
   END
$$ LANGUAGE plpgsql;

/* 
 * get the high-water mark of the local node
 */
CREATE OR REPLACE FUNCTION nodes.getLocalHigh() RETURNS TABLE( _nodeid uuid, _clockid bigint, _tsn bigint ) AS $$
   BEGIN
     RETURN QUERY
        select nodeid, clockid, currval( 'nodes.tsn') from nodes.clockid where clock = 0;
   END
$$ LANGUAGE plpgsql;

/*
 * write a new high-water mark record for a remote node
 */
CREATE OR REPLACE FUNCTION nodes.putRemoteHigh( _nodeid uuid, _clockid bigint, _tsn bigint) RETURNS VOID AS $$
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



