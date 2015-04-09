--
-- PostgreSQL database dump
--

-- Dumped from database version 9.3.6
-- Dumped by pg_dump version 9.3.6
-- Started on 2015-03-19 15:49:06 AEDT

SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;

--
-- TOC entry 4554 (class 1262 OID 1137362)
-- Dependencies: 4553
-- Name: gdf; Type: COMMENT; Schema: -; Owner: cube_admin
--

COMMENT ON DATABASE gdf IS 'Draft General Data Framework Development Database';


--
-- TOC entry 9 (class 2615 OID 1143816)
-- Name: earth_observation; Type: SCHEMA; Schema: -; Owner: cube_admin
--

CREATE SCHEMA earth_observation;


ALTER SCHEMA earth_observation OWNER TO cube_admin;

--
-- TOC entry 8 (class 2615 OID 1142288)
-- Name: topology; Type: SCHEMA; Schema: -; Owner: cube_admin
--

CREATE SCHEMA topology;


ALTER SCHEMA topology OWNER TO cube_admin;

--
-- TOC entry 7 (class 2615 OID 1140566)
-- Name: ztmp; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA ztmp;


ALTER SCHEMA ztmp OWNER TO postgres;

--
-- TOC entry 4557 (class 0 OID 0)
-- Dependencies: 7
-- Name: SCHEMA ztmp; Type: COMMENT; Schema: -; Owner: postgres
--

COMMENT ON SCHEMA ztmp IS 'Temporary schema';


--
-- TOC entry 232 (class 3079 OID 12617)
-- Name: plpgsql; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;


--
-- TOC entry 4559 (class 0 OID 0)
-- Dependencies: 232
-- Name: EXTENSION plpgsql; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';


--
-- TOC entry 231 (class 3079 OID 1142289)
-- Name: adminpack; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS adminpack WITH SCHEMA pg_catalog;


--
-- TOC entry 4560 (class 0 OID 0)
-- Dependencies: 231
-- Name: EXTENSION adminpack; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION adminpack IS 'administrative functions for PostgreSQL';


--
-- TOC entry 233 (class 3079 OID 1142298)
-- Name: postgis; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS postgis WITH SCHEMA public;


--
-- TOC entry 4561 (class 0 OID 0)
-- Dependencies: 233
-- Name: EXTENSION postgis; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION postgis IS 'PostGIS geometry, geography, and raster spatial types and functions';


--
-- TOC entry 234 (class 3079 OID 1143585)
-- Name: postgis_topology; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS postgis_topology WITH SCHEMA topology;


--
-- TOC entry 4562 (class 0 OID 0)
-- Dependencies: 234
-- Name: EXTENSION postgis_topology; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION postgis_topology IS 'PostGIS topology spatial types and functions';


SET search_path = public, pg_catalog;

--
-- TOC entry 1716 (class 1247 OID 1140569)
-- Name: attribute_value_type; Type: TYPE; Schema: public; Owner: cube_admin
--

CREATE TYPE attribute_value_type AS (
	attribute_type_id bigint,
	attribute_id bigint,
	attribute_value_id bigint
);


ALTER TYPE public.attribute_value_type OWNER TO cube_admin;

--
-- TOC entry 1719 (class 1247 OID 1140572)
-- Name: category_id_level_type; Type: TYPE; Schema: public; Owner: cube_admin
--

CREATE TYPE category_id_level_type AS (
	category_type_id bigint,
	category_id bigint,
	level integer
);


ALTER TYPE public.category_id_level_type OWNER TO cube_admin;

--
-- TOC entry 1722 (class 1247 OID 1140575)
-- Name: category_id_type; Type: TYPE; Schema: public; Owner: cube_admin
--

CREATE TYPE category_id_type AS (
	category_type_id bigint,
	category_id bigint
);


ALTER TYPE public.category_id_type OWNER TO cube_admin;

SET search_path = earth_observation, pg_catalog;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- TOC entry 225 (class 1259 OID 1143817)
-- Name: spectral_parameters; Type: TABLE; Schema: earth_observation; Owner: cube_admin; Tablespace: 
--

CREATE TABLE spectral_parameters (
    measurement_metatype_id bigint NOT NULL,
    measurement_type_id bigint NOT NULL
);


ALTER TABLE earth_observation.spectral_parameters OWNER TO cube_admin;

--
-- TOC entry 4563 (class 0 OID 0)
-- Dependencies: 225
-- Name: TABLE spectral_parameters; Type: COMMENT; Schema: earth_observation; Owner: cube_admin
--

COMMENT ON TABLE spectral_parameters IS 'Configuration: Spectral band parameters';


SET search_path = public, pg_catalog;

--
-- TOC entry 176 (class 1259 OID 1140576)
-- Name: dataset; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE dataset (
    dataset_type_id bigint NOT NULL,
    dataset_id bigint NOT NULL,
    observation_type_id bigint NOT NULL,
    observation_id bigint NOT NULL,
    dataset_location character varying(254)
);


ALTER TABLE public.dataset OWNER TO cube_admin;

--
-- TOC entry 4564 (class 0 OID 0)
-- Dependencies: 176
-- Name: TABLE dataset; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE dataset IS 'Data: Source dataset (file) ingested.
An example would be a dataset for a particular NBAR Landsat scene.';


--
-- TOC entry 177 (class 1259 OID 1140579)
-- Name: dataset_dimension; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE dataset_dimension (
    dataset_type_id bigint NOT NULL,
    dataset_id bigint NOT NULL,
    domain_id bigint NOT NULL,
    dimension_id bigint NOT NULL,
    min_value double precision NOT NULL,
    max_value double precision NOT NULL,
    indexing_value double precision
);


ALTER TABLE public.dataset_dimension OWNER TO cube_admin;

--
-- TOC entry 4565 (class 0 OID 0)
-- Dependencies: 177
-- Name: TABLE dataset_dimension; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE dataset_dimension IS 'Data: Dimensional parameters for each source dataset.
Each dataset/dimension will have specific max/min/indexing values showing the range covered by the dataset in that particular dimension.';


--
-- TOC entry 178 (class 1259 OID 1140582)
-- Name: dataset_metadata; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE dataset_metadata (
    dataset_type_id bigint NOT NULL,
    dataset_id bigint NOT NULL,
    metadata_xml xml NOT NULL
);


ALTER TABLE public.dataset_metadata OWNER TO cube_admin;

--
-- TOC entry 4566 (class 0 OID 0)
-- Dependencies: 178
-- Name: TABLE dataset_metadata; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE dataset_metadata IS 'Data: Lookup table for dataset-level metadata (one:one)';


--
-- TOC entry 179 (class 1259 OID 1140588)
-- Name: dataset_type; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE dataset_type (
    dataset_type_id bigint NOT NULL,
    dataset_type_name character varying(254)
);


ALTER TABLE public.dataset_type OWNER TO cube_admin;

--
-- TOC entry 4567 (class 0 OID 0)
-- Dependencies: 179
-- Name: TABLE dataset_type; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE dataset_type IS 'Configuration: Type of source dataset (processing level)';


--
-- TOC entry 180 (class 1259 OID 1140591)
-- Name: dataset_type_dimension; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE dataset_type_dimension (
    dataset_type_id bigint NOT NULL,
    domain_id bigint NOT NULL,
    dimension_id bigint NOT NULL
);


ALTER TABLE public.dataset_type_dimension OWNER TO cube_admin;

--
-- TOC entry 4568 (class 0 OID 0)
-- Dependencies: 180
-- Name: TABLE dataset_type_dimension; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE dataset_type_dimension IS 'Configuration: Association between dataset type and dimensions. Used to define dimensionality of source dataset types';


--
-- TOC entry 181 (class 1259 OID 1140594)
-- Name: dataset_type_domain; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE dataset_type_domain (
    dataset_type_id bigint NOT NULL,
    domain_id bigint NOT NULL,
    reference_system_id bigint
);


ALTER TABLE public.dataset_type_domain OWNER TO cube_admin;

--
-- TOC entry 4569 (class 0 OID 0)
-- Dependencies: 181
-- Name: TABLE dataset_type_domain; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE dataset_type_domain IS 'Configuration: Association between dataset types and domains (many-many).
Used to define which domains cover a given dataset type';


--
-- TOC entry 182 (class 1259 OID 1140597)
-- Name: dataset_type_measurement_type; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE dataset_type_measurement_type (
    dataset_type_id bigint NOT NULL,
    measurement_metatype_id bigint NOT NULL,
    measurement_type_id bigint NOT NULL,
    datatype_id smallint,
    measurement_type_index smallint
);


ALTER TABLE public.dataset_type_measurement_type OWNER TO cube_admin;

--
-- TOC entry 4570 (class 0 OID 0)
-- Dependencies: 182
-- Name: TABLE dataset_type_measurement_type; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE dataset_type_measurement_type IS 'Configuration: Associations between dataset types and measurement types (one-many)
e.g. associations between Landsat 7 NBAR and specific surface-reflectance corrected Landsat 7 bands';


--
-- TOC entry 183 (class 1259 OID 1140600)
-- Name: datatype; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE datatype (
    datatype_id smallint NOT NULL,
    datatype_name character varying(16),
    numpy_datatype_name character varying(16),
    gdal_datatype_name character varying(16),
    netcdf_datatype_name character varying(16)
);


ALTER TABLE public.datatype OWNER TO cube_admin;

--
-- TOC entry 4571 (class 0 OID 0)
-- Dependencies: 183
-- Name: TABLE datatype; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE datatype IS 'Configuration: Lookup table for measurement_type datatypes.';


--
-- TOC entry 184 (class 1259 OID 1140603)
-- Name: dimension; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE dimension (
    dimension_id bigint NOT NULL,
    dimension_name character varying(50) NOT NULL,
    dimension_tag character varying(8) NOT NULL
);


ALTER TABLE public.dimension OWNER TO cube_admin;

--
-- TOC entry 4572 (class 0 OID 0)
-- Dependencies: 184
-- Name: TABLE dimension; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE dimension IS 'Configuration: Dimensions for n-dimensional data structures, e.g. x,y,z,t';


--
-- TOC entry 185 (class 1259 OID 1140606)
-- Name: dimension_domain; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE dimension_domain (
    domain_id bigint NOT NULL,
    dimension_id bigint NOT NULL
);


ALTER TABLE public.dimension_domain OWNER TO cube_admin;

--
-- TOC entry 4573 (class 0 OID 0)
-- Dependencies: 185
-- Name: TABLE dimension_domain; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE dimension_domain IS 'Configuration: Many-many  mapping between dimensions and domains to allow multiple dimensions to be included in multiple domains.
For example, the z dimension could be managed in a Z-spatial domain, or in an XYZ-spatial domain.';


--
-- TOC entry 186 (class 1259 OID 1140609)
-- Name: domain; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE domain (
    domain_id bigint NOT NULL,
    domain_name character varying(16)
);


ALTER TABLE public.domain OWNER TO cube_admin;

--
-- TOC entry 4574 (class 0 OID 0)
-- Dependencies: 186
-- Name: TABLE domain; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE domain IS 'Configuration: Domain groupings of dimensions (e.g. spectral, spatial XY, spatial XYZ, temporal)';


--
-- TOC entry 224 (class 1259 OID 1143790)
-- Name: indexing_type; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE indexing_type (
    indexing_type_id smallint NOT NULL,
    indexing_type_name character varying(128)
);


ALTER TABLE public.indexing_type OWNER TO cube_admin;

--
-- TOC entry 4575 (class 0 OID 0)
-- Dependencies: 224
-- Name: TABLE indexing_type; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE indexing_type IS 'Configuration: Lookup table to manage what kind of indexing to apply to a given dimension';


--
-- TOC entry 187 (class 1259 OID 1140612)
-- Name: ndarray_type; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE ndarray_type (
    ndarray_type_id bigint NOT NULL,
    ndarray_type_name character varying(254),
    ndarray_type_tag character varying(16)
);


ALTER TABLE public.ndarray_type OWNER TO cube_admin;

--
-- TOC entry 4576 (class 0 OID 0)
-- Dependencies: 187
-- Name: TABLE ndarray_type; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE ndarray_type IS 'Configuration: ndarray parameter lookup table. Used to manage different ndarray_types';


--
-- TOC entry 188 (class 1259 OID 1140615)
-- Name: ndarray_type_dimension; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE ndarray_type_dimension (
    ndarray_type_id bigint NOT NULL,
    domain_id bigint NOT NULL,
    dimension_id bigint NOT NULL,
    creation_order smallint,
    dimension_extent numeric,
    dimension_elements bigint,
    dimension_cache bigint,
    dimension_origin numeric,
    indexing_type_id smallint,
    reference_system_id bigint,
    dimension_extent_unit character varying(32)
);


ALTER TABLE public.ndarray_type_dimension OWNER TO cube_admin;

--
-- TOC entry 4577 (class 0 OID 0)
-- Dependencies: 188
-- Name: TABLE ndarray_type_dimension; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE ndarray_type_dimension IS 'Configuration: Association between attribute set and dimensions. Used to define dimensionality of ndarrays using a given attribute set';


--
-- TOC entry 189 (class 1259 OID 1140624)
-- Name: reference_system; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE reference_system (
    reference_system_id bigint NOT NULL,
    reference_system_name character varying(32),
    reference_system_unit character varying(32),
    reference_system_definition character varying(254),
    reference_system_tag character varying(32)
);


ALTER TABLE public.reference_system OWNER TO cube_admin;

--
-- TOC entry 4578 (class 0 OID 0)
-- Dependencies: 189
-- Name: TABLE reference_system; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE reference_system IS 'Configuration: Coordinate reference systems for aplication to specific domains.
e.g. EPSG:4326, seconds since 1/1/1970 0:00, etc.';


--
-- TOC entry 228 (class 1259 OID 1143964)
-- Name: ndarray_type_dimension_view; Type: MATERIALIZED VIEW; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE MATERIALIZED VIEW ndarray_type_dimension_view AS
 SELECT ndarray_type.ndarray_type_id,
    ndarray_type_dimension.creation_order,
    domain.domain_id,
    dimension.dimension_id,
    ndarray_type_dimension.reference_system_id,
    ndarray_type.ndarray_type_name,
    dimension.dimension_name,
    dimension.dimension_tag,
    domain.domain_name,
    reference_system.reference_system_name,
    reference_system.reference_system_definition,
    indexing_type.indexing_type_name,
    ndarray_type_dimension.dimension_origin,
    ndarray_type_dimension.dimension_extent,
    COALESCE(ndarray_type_dimension.dimension_extent_unit, reference_system.reference_system_unit) AS dimension_extent_unit,
    ndarray_type_dimension.dimension_elements,
    reference_system.reference_system_unit,
    ndarray_type_dimension.dimension_cache
   FROM ((((((ndarray_type
     JOIN ndarray_type_dimension USING (ndarray_type_id))
     JOIN dimension_domain USING (domain_id, dimension_id))
     JOIN domain USING (domain_id))
     JOIN dimension USING (dimension_id))
     JOIN reference_system USING (reference_system_id))
     JOIN indexing_type USING (indexing_type_id))
  ORDER BY ndarray_type.ndarray_type_id, ndarray_type_dimension.creation_order
  WITH NO DATA;


ALTER TABLE public.ndarray_type_dimension_view OWNER TO cube_admin;

--
-- TOC entry 190 (class 1259 OID 1140635)
-- Name: reference_system_indexing; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE reference_system_indexing (
    reference_system_id bigint NOT NULL,
    array_index integer NOT NULL,
    indexing_name character varying(64),
    measurement_metatype_id bigint,
    measurement_type_id bigint
);


ALTER TABLE public.reference_system_indexing OWNER TO cube_admin;

--
-- TOC entry 4579 (class 0 OID 0)
-- Dependencies: 190
-- Name: TABLE reference_system_indexing; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE reference_system_indexing IS 'Configuration: Optional non-linear indexing for dimension in a given domain.
e.g. A spectral dimension containing multple bands needs to be indexed by band number, and each band number can be associated with a given measurement_type.';


--
-- TOC entry 229 (class 1259 OID 1143972)
-- Name: dimension_indices_view; Type: MATERIALIZED VIEW; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE MATERIALIZED VIEW dimension_indices_view AS
 SELECT ndarray_type_dimension_view.ndarray_type_id,
    ndarray_type_dimension_view.domain_id,
    ndarray_type_dimension_view.dimension_id,
    reference_system_indexing.reference_system_id,
    reference_system_indexing.array_index,
    reference_system_indexing.indexing_name,
    reference_system_indexing.measurement_metatype_id,
    reference_system_indexing.measurement_type_id
   FROM (ndarray_type_dimension_view
     JOIN reference_system_indexing USING (reference_system_id))
  ORDER BY ndarray_type_dimension_view.ndarray_type_id, ndarray_type_dimension_view.dimension_id, reference_system_indexing.array_index
  WITH NO DATA;


ALTER TABLE public.dimension_indices_view OWNER TO cube_admin;

--
-- TOC entry 226 (class 1259 OID 1143849)
-- Name: ndarray_type_dimension_property; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE ndarray_type_dimension_property (
    ndarray_type_id bigint NOT NULL,
    domain_id bigint NOT NULL,
    dimension_id bigint NOT NULL,
    property_id bigint NOT NULL,
    attribute_string character varying(128) NOT NULL
);


ALTER TABLE public.ndarray_type_dimension_property OWNER TO cube_admin;

--
-- TOC entry 227 (class 1259 OID 1143877)
-- Name: property; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE property (
    property_id bigint NOT NULL,
    property_name character varying(32),
    datatype_id smallint
);


ALTER TABLE public.property OWNER TO cube_admin;

--
-- TOC entry 230 (class 1259 OID 1143976)
-- Name: dimension_properties_view; Type: MATERIALIZED VIEW; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE MATERIALIZED VIEW dimension_properties_view AS
 SELECT ndarray_type_dimension_view.ndarray_type_id,
    ndarray_type_dimension_view.domain_id,
    ndarray_type_dimension_view.dimension_id,
    ndarray_type_dimension_view.dimension_name,
    property.property_name,
    ndarray_type_dimension_property.attribute_string,
    datatype.datatype_name
   FROM (((ndarray_type_dimension_view
     JOIN ndarray_type_dimension_property USING (ndarray_type_id, domain_id, dimension_id))
     JOIN property USING (property_id))
     JOIN datatype USING (datatype_id))
  ORDER BY ndarray_type_dimension_view.ndarray_type_id, ndarray_type_dimension_view.creation_order, property.property_name
  WITH NO DATA;


ALTER TABLE public.dimension_properties_view OWNER TO cube_admin;

--
-- TOC entry 191 (class 1259 OID 1140642)
-- Name: instrument; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE instrument (
    instrument_type_id bigint NOT NULL,
    instrument_id bigint NOT NULL,
    instrument_name character varying(128),
    platform_type_id bigint,
    platform_id bigint
);


ALTER TABLE public.instrument OWNER TO cube_admin;

--
-- TOC entry 4580 (class 0 OID 0)
-- Dependencies: 191
-- Name: TABLE instrument; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE instrument IS 'Configuration: Instrument used to gather observations.
An example would be the ETM+ sensor on the Landsat 7 platform';


--
-- TOC entry 192 (class 1259 OID 1140645)
-- Name: instrument_type; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE instrument_type (
    instrument_type_id bigint NOT NULL,
    instrument_type_name character varying(128)
);


ALTER TABLE public.instrument_type OWNER TO cube_admin;

--
-- TOC entry 4581 (class 0 OID 0)
-- Dependencies: 192
-- Name: TABLE instrument_type; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE instrument_type IS 'Configuration: Lookup table for instrument category';


--
-- TOC entry 194 (class 1259 OID 1140651)
-- Name: measurement_metatype; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE measurement_metatype (
    measurement_metatype_id bigint NOT NULL,
    measurement_metatype_name character varying(254)
);


ALTER TABLE public.measurement_metatype OWNER TO cube_admin;

--
-- TOC entry 4582 (class 0 OID 0)
-- Dependencies: 194
-- Name: TABLE measurement_metatype; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE measurement_metatype IS 'Configuration: General type of measurement (e.g. spectral band)';


--
-- TOC entry 193 (class 1259 OID 1140648)
-- Name: measurement_type; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE measurement_type (
    measurement_metatype_id bigint NOT NULL,
    measurement_type_id bigint NOT NULL,
    measurement_type_name character varying(50) NOT NULL,
    measurement_type_tag character varying(16)
);


ALTER TABLE public.measurement_type OWNER TO cube_admin;

--
-- TOC entry 4583 (class 0 OID 0)
-- Dependencies: 193
-- Name: TABLE measurement_type; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE measurement_type IS 'Configuration: Description of measurement(s) held in n-dimensional data structures: e.g. bands';


--
-- TOC entry 195 (class 1259 OID 1140654)
-- Name: ndarray; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE ndarray (
    ndarray_type_id bigint NOT NULL,
    ndarray_id bigint NOT NULL,
    ndarray_version integer NOT NULL,
    ndarray_location character varying(354),
    md5_checksum character(32),
    ndarray_bytes bigint
);


ALTER TABLE public.ndarray OWNER TO cube_admin;

--
-- TOC entry 4584 (class 0 OID 0)
-- Dependencies: 195
-- Name: TABLE ndarray; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE ndarray IS 'Data: n-dimensional data structure instances';


--
-- TOC entry 196 (class 1259 OID 1140657)
-- Name: ndarray_dataset; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE ndarray_dataset (
    ndarray_type_id bigint NOT NULL,
    ndarray_id bigint NOT NULL,
    ndarray_version integer NOT NULL,
    dataset_type_id bigint NOT NULL,
    dataset_id bigint NOT NULL
);


ALTER TABLE public.ndarray_dataset OWNER TO cube_admin;

--
-- TOC entry 4585 (class 0 OID 0)
-- Dependencies: 196
-- Name: TABLE ndarray_dataset; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE ndarray_dataset IS 'Data: Association between ndarray and dataset instances (many-many)';


--
-- TOC entry 197 (class 1259 OID 1140660)
-- Name: ndarray_dimension; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE ndarray_dimension (
    ndarray_type_id bigint NOT NULL,
    ndarray_id bigint NOT NULL,
    ndarray_version integer NOT NULL,
    domain_id bigint NOT NULL,
    dimension_id bigint NOT NULL,
    ndarray_dimension_index integer NOT NULL
);


ALTER TABLE public.ndarray_dimension OWNER TO cube_admin;

--
-- TOC entry 4586 (class 0 OID 0)
-- Dependencies: 197
-- Name: TABLE ndarray_dimension; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE ndarray_dimension IS 'Data: Association between ndarray and dimensions. Used to define attributes for each dimension in ndarray instances';


--
-- TOC entry 222 (class 1259 OID 1143735)
-- Name: ndarray_footprint; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE ndarray_footprint (
    ndarray_type_id bigint NOT NULL,
    ndarray_footprint_id bigint NOT NULL,
    bounding_box geometry
);


ALTER TABLE public.ndarray_footprint OWNER TO cube_admin;

--
-- TOC entry 4587 (class 0 OID 0)
-- Dependencies: 222
-- Name: TABLE ndarray_footprint; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE ndarray_footprint IS 'Data: Spatial footprint for tiles with a given set of indices in the XY dimensions';


--
-- TOC entry 223 (class 1259 OID 1143758)
-- Name: ndarray_footprint_dimension; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE ndarray_footprint_dimension (
    ndarray_type_id bigint NOT NULL,
    ndarray_footprint_id bigint NOT NULL,
    domain_id bigint NOT NULL,
    dimension_id bigint NOT NULL,
    dimension_index integer NOT NULL
);


ALTER TABLE public.ndarray_footprint_dimension OWNER TO cube_admin;

--
-- TOC entry 4588 (class 0 OID 0)
-- Dependencies: 223
-- Name: TABLE ndarray_footprint_dimension; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE ndarray_footprint_dimension IS 'Data: Association between ndarray footprint and dimensions';


--
-- TOC entry 198 (class 1259 OID 1140663)
-- Name: ndarray_type_measurement_type; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE ndarray_type_measurement_type (
    ndarray_type_id bigint NOT NULL,
    measurement_metatype_id bigint NOT NULL,
    measurement_type_id bigint NOT NULL,
    datatype_id smallint,
    measurement_type_index smallint
);


ALTER TABLE public.ndarray_type_measurement_type OWNER TO cube_admin;

--
-- TOC entry 4589 (class 0 OID 0)
-- Dependencies: 198
-- Name: TABLE ndarray_type_measurement_type; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE ndarray_type_measurement_type IS 'Configuration: Associations between n-dimensional data structure types and measurement types (many-many)
e.g. associations between Landsat 7 arrays and Landsat 7 bands if they are stored in separate variables';


--
-- TOC entry 199 (class 1259 OID 1140666)
-- Name: observation; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE observation (
    observation_type_id bigint NOT NULL,
    observation_id bigint NOT NULL,
    observation_start_datetime timestamp with time zone,
    observation_end_datetime timestamp with time zone,
    instrument_type_id bigint,
    instrument_id bigint
);


ALTER TABLE public.observation OWNER TO cube_admin;

--
-- TOC entry 4590 (class 0 OID 0)
-- Dependencies: 199
-- Name: TABLE observation; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE observation IS 'Data: Source observation for datasets.
Analagous to old "acquisition" table in AGDC version 0 DB';


--
-- TOC entry 200 (class 1259 OID 1140669)
-- Name: observation_type; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE observation_type (
    observation_type_id bigint NOT NULL,
    observation_type_name character varying(254)
);


ALTER TABLE public.observation_type OWNER TO cube_admin;

--
-- TOC entry 4591 (class 0 OID 0)
-- Dependencies: 200
-- Name: TABLE observation_type; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE observation_type IS 'Configuration: Type of source observation';


--
-- TOC entry 201 (class 1259 OID 1140672)
-- Name: platform; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE platform (
    platform_type_id bigint NOT NULL,
    platform_id bigint NOT NULL,
    platform_name character varying(128)
);


ALTER TABLE public.platform OWNER TO cube_admin;

--
-- TOC entry 4592 (class 0 OID 0)
-- Dependencies: 201
-- Name: TABLE platform; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE platform IS 'Configuration: Platform on which instrument is mounted.
An example would be a specific satellite such as Landsat 7';


--
-- TOC entry 202 (class 1259 OID 1140675)
-- Name: platform_type; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE platform_type (
    platform_type_id bigint NOT NULL,
    platform_type_name character varying(128)
);


ALTER TABLE public.platform_type OWNER TO cube_admin;

--
-- TOC entry 4593 (class 0 OID 0)
-- Dependencies: 202
-- Name: TABLE platform_type; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE platform_type IS 'Configuration: Lookup table for platform category
e.g. Satellite or Ship';


SET search_path = earth_observation, pg_catalog;

--
-- TOC entry 4543 (class 0 OID 1143817)
-- Dependencies: 225
-- Data for Name: spectral_parameters; Type: TABLE DATA; Schema: earth_observation; Owner: cube_admin
--



SET search_path = public, pg_catalog;

--
-- TOC entry 4513 (class 0 OID 1140576)
-- Dependencies: 176
-- Data for Name: dataset; Type: TABLE DATA; Schema: public; Owner: cube_admin
--



--
-- TOC entry 4514 (class 0 OID 1140579)
-- Dependencies: 177
-- Data for Name: dataset_dimension; Type: TABLE DATA; Schema: public; Owner: cube_admin
--



--
-- TOC entry 4515 (class 0 OID 1140582)
-- Dependencies: 178
-- Data for Name: dataset_metadata; Type: TABLE DATA; Schema: public; Owner: cube_admin
--



--
-- TOC entry 4516 (class 0 OID 1140588)
-- Dependencies: 179
-- Data for Name: dataset_type; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO dataset_type VALUES (1, 'L1T');
INSERT INTO dataset_type VALUES (2, 'NBAR');
INSERT INTO dataset_type VALUES (3, 'PQ');
INSERT INTO dataset_type VALUES (4, 'FC');


--
-- TOC entry 4517 (class 0 OID 1140591)
-- Dependencies: 180
-- Data for Name: dataset_type_dimension; Type: TABLE DATA; Schema: public; Owner: cube_admin
--



--
-- TOC entry 4518 (class 0 OID 1140594)
-- Dependencies: 181
-- Data for Name: dataset_type_domain; Type: TABLE DATA; Schema: public; Owner: cube_admin
--



--
-- TOC entry 4519 (class 0 OID 1140597)
-- Dependencies: 182
-- Data for Name: dataset_type_measurement_type; Type: TABLE DATA; Schema: public; Owner: cube_admin
--



--
-- TOC entry 4520 (class 0 OID 1140600)
-- Dependencies: 183
-- Data for Name: datatype; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO datatype VALUES (3, 'string', NULL, NULL, NULL);
INSERT INTO datatype VALUES (1, 'int16', 'int16', 'int16', 'i2');
INSERT INTO datatype VALUES (2, 'float32', 'float32', 'float32', 'f4');


--
-- TOC entry 4521 (class 0 OID 1140603)
-- Dependencies: 184
-- Data for Name: dimension; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO dimension VALUES (1, 'longitude', 'X');
INSERT INTO dimension VALUES (2, 'latitude', 'Y');
INSERT INTO dimension VALUES (3, 'height/depth', 'Z');
INSERT INTO dimension VALUES (5, 'spectral', 'LAMBDA');
INSERT INTO dimension VALUES (4, 'time', 'T');


--
-- TOC entry 4522 (class 0 OID 1140606)
-- Dependencies: 185
-- Data for Name: dimension_domain; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO dimension_domain VALUES (1, 1);
INSERT INTO dimension_domain VALUES (1, 2);
INSERT INTO dimension_domain VALUES (2, 3);
INSERT INTO dimension_domain VALUES (3, 4);
INSERT INTO dimension_domain VALUES (4, 5);
INSERT INTO dimension_domain VALUES (5, 1);
INSERT INTO dimension_domain VALUES (5, 2);
INSERT INTO dimension_domain VALUES (5, 3);


--
-- TOC entry 4523 (class 0 OID 1140609)
-- Dependencies: 186
-- Data for Name: domain; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO domain VALUES (1, 'Spatial XY');
INSERT INTO domain VALUES (2, 'Spatial Z');
INSERT INTO domain VALUES (3, 'Temporal');
INSERT INTO domain VALUES (4, 'Spectral');
INSERT INTO domain VALUES (5, 'Spatial XYZ');


--
-- TOC entry 4542 (class 0 OID 1143790)
-- Dependencies: 224
-- Data for Name: indexing_type; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO indexing_type VALUES (1, 'regular');
INSERT INTO indexing_type VALUES (2, 'irregular');
INSERT INTO indexing_type VALUES (3, 'fixed');


--
-- TOC entry 4528 (class 0 OID 1140642)
-- Dependencies: 191
-- Data for Name: instrument; Type: TABLE DATA; Schema: public; Owner: cube_admin
--



--
-- TOC entry 4529 (class 0 OID 1140645)
-- Dependencies: 192
-- Data for Name: instrument_type; Type: TABLE DATA; Schema: public; Owner: cube_admin
--



--
-- TOC entry 4531 (class 0 OID 1140651)
-- Dependencies: 194
-- Data for Name: measurement_metatype; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO measurement_metatype VALUES (10, 'Multi-band Spectral Radiance');
INSERT INTO measurement_metatype VALUES (1, 'Spectral Radiance (Single Band)');


--
-- TOC entry 4530 (class 0 OID 1140648)
-- Dependencies: 193
-- Data for Name: measurement_type; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO measurement_type VALUES (1, 56, 'Band 6 - Thermal Infrared', 'B60');
INSERT INTO measurement_type VALUES (1, 58, 'Band 8 - Panchromatic', 'B80');
INSERT INTO measurement_type VALUES (1, 78, 'Band 8 - Panchromatic', 'B80');
INSERT INTO measurement_type VALUES (1, 51, 'Band 1 - Visible Blue', 'B10');
INSERT INTO measurement_type VALUES (1, 52, 'Band 2 - Visible Green', 'B20');
INSERT INTO measurement_type VALUES (1, 72, 'Band 2 - Visible Green', 'B20');
INSERT INTO measurement_type VALUES (1, 71, 'Band 1 - Visible Blue', 'B10');
INSERT INTO measurement_type VALUES (1, 81, 'Band 1 - Coastal Aerosol', 'B1');
INSERT INTO measurement_type VALUES (1, 82, 'Band 2 - Visible Blue', 'B2');
INSERT INTO measurement_type VALUES (1, 53, 'Band 3 - Visible Red', 'B30');
INSERT INTO measurement_type VALUES (1, 73, 'Band 3 - Visible Red', 'B30');
INSERT INTO measurement_type VALUES (1, 84, 'Band 4 - Visible Red', 'B4');
INSERT INTO measurement_type VALUES (1, 83, 'Band 3 - Visible Green', 'B3');
INSERT INTO measurement_type VALUES (1, 54, 'Band 4 - Near Infrared', 'B40');
INSERT INTO measurement_type VALUES (1, 74, 'Band 4 - Near Infrared', 'B40');
INSERT INTO measurement_type VALUES (1, 85, 'Band 5 - Near Infrared', 'B5');
INSERT INTO measurement_type VALUES (1, 55, 'Band 5 - Middle Infrared 1', 'B50');
INSERT INTO measurement_type VALUES (1, 75, 'Band 5 - Middle Infrared 1', 'B50');
INSERT INTO measurement_type VALUES (1, 77, 'Band 7 - Middle Infrared 2', 'B70');
INSERT INTO measurement_type VALUES (1, 57, 'Band 7 - Middle Infrared 2', 'B70');
INSERT INTO measurement_type VALUES (1, 86, 'Band 6 - Short-wave Infrared 1', 'B6');
INSERT INTO measurement_type VALUES (1, 87, 'Band 7 - Short-wave Infrared 2', 'B7');
INSERT INTO measurement_type VALUES (1, 88, 'Band 8 - Panchromatic', 'B8');
INSERT INTO measurement_type VALUES (1, 89, 'Band 9 - Cirrus', 'B9');
INSERT INTO measurement_type VALUES (1, 761, 'Band 61 - Thermal Infrared Low Gain', 'B61');
INSERT INTO measurement_type VALUES (1, 762, 'Band 62 - Thermal Infrared High Gain', 'B62');
INSERT INTO measurement_type VALUES (1, 810, 'Band 10 - Thermal Infrared 1', 'B10');
INSERT INTO measurement_type VALUES (1, 811, 'Band 11 - Thermal Infrared 2', 'B11');
INSERT INTO measurement_type VALUES (1, 101, 'Photosynthetic Vegetation', 'PV');
INSERT INTO measurement_type VALUES (1, 102, 'Non-Photosynthetic Vegetation', 'NPV');
INSERT INTO measurement_type VALUES (1, 103, 'Bare Soil', 'BS');
INSERT INTO measurement_type VALUES (1, 104, 'Unmixing Error', 'UE');
INSERT INTO measurement_type VALUES (10, 1000, 'Multi-Band Spectral Radiance', 'SPECTRAL');


--
-- TOC entry 4532 (class 0 OID 1140654)
-- Dependencies: 195
-- Data for Name: ndarray; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO ndarray VALUES (5, 1, 0, 'test_location', NULL, NULL);


--
-- TOC entry 4533 (class 0 OID 1140657)
-- Dependencies: 196
-- Data for Name: ndarray_dataset; Type: TABLE DATA; Schema: public; Owner: cube_admin
--



--
-- TOC entry 4534 (class 0 OID 1140660)
-- Dependencies: 197
-- Data for Name: ndarray_dimension; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO ndarray_dimension VALUES (5, 1, 0, 1, 1, 140);
INSERT INTO ndarray_dimension VALUES (5, 1, 0, 1, 2, -35);
INSERT INTO ndarray_dimension VALUES (5, 1, 0, 3, 4, 2010);


--
-- TOC entry 4540 (class 0 OID 1143735)
-- Dependencies: 222
-- Data for Name: ndarray_footprint; Type: TABLE DATA; Schema: public; Owner: cube_admin
--



--
-- TOC entry 4541 (class 0 OID 1143758)
-- Dependencies: 223
-- Data for Name: ndarray_footprint_dimension; Type: TABLE DATA; Schema: public; Owner: cube_admin
--



--
-- TOC entry 4524 (class 0 OID 1140612)
-- Dependencies: 187
-- Data for Name: ndarray_type; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO ndarray_type VALUES (5, 'Landsat 5 TM ARG-25', 'LS5TM');
INSERT INTO ndarray_type VALUES (7, 'Landsat 7 ETM ARG-25', 'LS7ETM');
INSERT INTO ndarray_type VALUES (8, 'Landsat 8 OLI ARG-25', 'LS8OLI');
INSERT INTO ndarray_type VALUES (82, 'Landsat 8 TIRS', 'LS8TIRS');
INSERT INTO ndarray_type VALUES (50, 'Landsat 5 TM ARG-25 with spectral dimension', 'LS5TM-SD');


--
-- TOC entry 4525 (class 0 OID 1140615)
-- Dependencies: 188
-- Data for Name: ndarray_type_dimension; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO ndarray_type_dimension VALUES (5, 1, 1, 2, 1.0, 4000, 128, 0.0, 1, 4326, NULL);
INSERT INTO ndarray_type_dimension VALUES (5, 1, 2, 3, 1.0, 4000, 128, 0.0, 1, 4326, NULL);
INSERT INTO ndarray_type_dimension VALUES (50, 1, 1, 3, 1.0, 4000, 128, 0.0, 1, 4326, NULL);
INSERT INTO ndarray_type_dimension VALUES (50, 1, 2, 4, 1.0, 4000, 128, 0.0, 1, 4326, NULL);
INSERT INTO ndarray_type_dimension VALUES (50, 4, 5, 1, NULL, NULL, NULL, NULL, 3, 50, NULL);
INSERT INTO ndarray_type_dimension VALUES (5, 3, 4, 1, 1.0, 31622400, 128, 0.0, 2, 4, 'year');
INSERT INTO ndarray_type_dimension VALUES (50, 3, 4, 2, 1.0, 31622400, 128, 0.0, 2, 4, 'year');


--
-- TOC entry 4544 (class 0 OID 1143849)
-- Dependencies: 226
-- Data for Name: ndarray_type_dimension_property; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO ndarray_type_dimension_property VALUES (5, 1, 1, 1, 'longitude');
INSERT INTO ndarray_type_dimension_property VALUES (5, 1, 1, 2, 'longitude');
INSERT INTO ndarray_type_dimension_property VALUES (5, 1, 1, 3, 'degrees_east');
INSERT INTO ndarray_type_dimension_property VALUES (5, 1, 1, 4, 'X');
INSERT INTO ndarray_type_dimension_property VALUES (5, 3, 4, 5, 'gregorian');
INSERT INTO ndarray_type_dimension_property VALUES (5, 1, 2, 1, 'latitude');
INSERT INTO ndarray_type_dimension_property VALUES (5, 1, 2, 2, 'latitude');
INSERT INTO ndarray_type_dimension_property VALUES (5, 1, 2, 3, 'degrees_north');
INSERT INTO ndarray_type_dimension_property VALUES (5, 1, 2, 4, 'Y');
INSERT INTO ndarray_type_dimension_property VALUES (5, 3, 4, 1, 'time');
INSERT INTO ndarray_type_dimension_property VALUES (5, 3, 4, 2, 'time');
INSERT INTO ndarray_type_dimension_property VALUES (5, 3, 4, 3, 'seconds since 1970-01-01 ');
INSERT INTO ndarray_type_dimension_property VALUES (5, 3, 4, 4, 'T');


--
-- TOC entry 4535 (class 0 OID 1140663)
-- Dependencies: 198
-- Data for Name: ndarray_type_measurement_type; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO ndarray_type_measurement_type VALUES (5, 1, 51, 1, 1);
INSERT INTO ndarray_type_measurement_type VALUES (5, 1, 52, 1, 2);
INSERT INTO ndarray_type_measurement_type VALUES (5, 1, 53, 1, 3);
INSERT INTO ndarray_type_measurement_type VALUES (5, 1, 54, 1, 4);
INSERT INTO ndarray_type_measurement_type VALUES (5, 1, 55, 1, 5);
INSERT INTO ndarray_type_measurement_type VALUES (5, 1, 57, 1, 6);
INSERT INTO ndarray_type_measurement_type VALUES (7, 1, 71, 1, 1);
INSERT INTO ndarray_type_measurement_type VALUES (7, 1, 72, 1, 2);
INSERT INTO ndarray_type_measurement_type VALUES (7, 1, 73, 1, 3);
INSERT INTO ndarray_type_measurement_type VALUES (7, 1, 74, 1, 4);
INSERT INTO ndarray_type_measurement_type VALUES (7, 1, 75, 1, 5);
INSERT INTO ndarray_type_measurement_type VALUES (7, 1, 77, 1, 6);
INSERT INTO ndarray_type_measurement_type VALUES (50, 10, 1000, 1, 1);


--
-- TOC entry 4536 (class 0 OID 1140666)
-- Dependencies: 199
-- Data for Name: observation; Type: TABLE DATA; Schema: public; Owner: cube_admin
--



--
-- TOC entry 4537 (class 0 OID 1140669)
-- Dependencies: 200
-- Data for Name: observation_type; Type: TABLE DATA; Schema: public; Owner: cube_admin
--



--
-- TOC entry 4538 (class 0 OID 1140672)
-- Dependencies: 201
-- Data for Name: platform; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO platform VALUES (1, 5, 'Landsat 5');
INSERT INTO platform VALUES (1, 7, 'Landsat 7');
INSERT INTO platform VALUES (1, 8, 'Landsat 8');


--
-- TOC entry 4539 (class 0 OID 1140675)
-- Dependencies: 202
-- Data for Name: platform_type; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO platform_type VALUES (1, 'Satellite');


--
-- TOC entry 4545 (class 0 OID 1143877)
-- Dependencies: 227
-- Data for Name: property; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO property VALUES (4, 'axis', 3);
INSERT INTO property VALUES (2, 'standard_name', 3);
INSERT INTO property VALUES (1, 'long_name', 3);
INSERT INTO property VALUES (3, 'units', 3);
INSERT INTO property VALUES (5, 'calendar', 3);


--
-- TOC entry 4526 (class 0 OID 1140624)
-- Dependencies: 189
-- Data for Name: reference_system; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO reference_system VALUES (4326, 'Unprojected WGS84 (Lat-long)', 'degrees', 'EPSG:4326', NULL);
INSERT INTO reference_system VALUES (3, 'Australian Height Datum (AHD)', 'metres', 'AHD', NULL);
INSERT INTO reference_system VALUES (4, 'Seconds since 1/1/1970 0:00', 'seconds', 'SSE', NULL);
INSERT INTO reference_system VALUES (70, 'Landsat 7 Spectral Bands', 'band', 'LS7', NULL);
INSERT INTO reference_system VALUES (80, 'Landsat 8 Band', 'band', 'LS8', NULL);
INSERT INTO reference_system VALUES (50, 'Landsat 5 Reflectance Bands', 'band', 'LS5', NULL);


--
-- TOC entry 4527 (class 0 OID 1140635)
-- Dependencies: 190
-- Data for Name: reference_system_indexing; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO reference_system_indexing VALUES (50, 0, 'Band 1 - Visible Blue', 1, 51);
INSERT INTO reference_system_indexing VALUES (50, 1, 'Band 2 - Visible Green', 1, 52);
INSERT INTO reference_system_indexing VALUES (50, 2, 'Band 3 - Visible Red', 1, 53);
INSERT INTO reference_system_indexing VALUES (50, 3, 'Band 4 - Near Infrared', 1, 54);
INSERT INTO reference_system_indexing VALUES (50, 4, 'Band 5 - Middle Infrared 1', 1, 55);
INSERT INTO reference_system_indexing VALUES (50, 5, 'Band 7 - Middle Infrared 2', 1, 57);


--
-- TOC entry 4205 (class 0 OID 1142566)
-- Dependencies: 204
-- Data for Name: spatial_ref_sys; Type: TABLE DATA; Schema: public; Owner: cube_admin
--



SET search_path = topology, pg_catalog;

--
-- TOC entry 4207 (class 0 OID 1143601)
-- Dependencies: 218
-- Data for Name: layer; Type: TABLE DATA; Schema: topology; Owner: cube_admin
--



--
-- TOC entry 4206 (class 0 OID 1143588)
-- Dependencies: 217
-- Data for Name: topology; Type: TABLE DATA; Schema: topology; Owner: cube_admin
--



SET search_path = earth_observation, pg_catalog;

--
-- TOC entry 4347 (class 2606 OID 1143821)
-- Name: pk_spectral_parameters; Type: CONSTRAINT; Schema: earth_observation; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY spectral_parameters
    ADD CONSTRAINT pk_spectral_parameters PRIMARY KEY (measurement_metatype_id, measurement_type_id);


SET search_path = public, pg_catalog;

--
-- TOC entry 4215 (class 2606 OID 1140679)
-- Name: pk_dataset; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dataset
    ADD CONSTRAINT pk_dataset PRIMARY KEY (dataset_type_id, dataset_id);


--
-- TOC entry 4219 (class 2606 OID 1140681)
-- Name: pk_dataset_dimension; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dataset_dimension
    ADD CONSTRAINT pk_dataset_dimension PRIMARY KEY (dataset_type_id, dataset_id, domain_id, dimension_id);


--
-- TOC entry 4222 (class 2606 OID 1140683)
-- Name: pk_dataset_metadata; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dataset_metadata
    ADD CONSTRAINT pk_dataset_metadata PRIMARY KEY (dataset_type_id, dataset_id);


--
-- TOC entry 4224 (class 2606 OID 1140685)
-- Name: pk_dataset_type; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dataset_type
    ADD CONSTRAINT pk_dataset_type PRIMARY KEY (dataset_type_id);


--
-- TOC entry 4230 (class 2606 OID 1140687)
-- Name: pk_dataset_type_dimension; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dataset_type_dimension
    ADD CONSTRAINT pk_dataset_type_dimension PRIMARY KEY (dataset_type_id, domain_id, dimension_id);


--
-- TOC entry 4235 (class 2606 OID 1140689)
-- Name: pk_dataset_type_domain; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dataset_type_domain
    ADD CONSTRAINT pk_dataset_type_domain PRIMARY KEY (dataset_type_id, domain_id);


--
-- TOC entry 4240 (class 2606 OID 1140691)
-- Name: pk_dataset_type_measurement_type; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dataset_type_measurement_type
    ADD CONSTRAINT pk_dataset_type_measurement_type PRIMARY KEY (dataset_type_id, measurement_metatype_id, measurement_type_id);


--
-- TOC entry 4244 (class 2606 OID 1140693)
-- Name: pk_datatype; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY datatype
    ADD CONSTRAINT pk_datatype PRIMARY KEY (datatype_id);


--
-- TOC entry 4248 (class 2606 OID 1140695)
-- Name: pk_dimension; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dimension
    ADD CONSTRAINT pk_dimension PRIMARY KEY (dimension_id);


--
-- TOC entry 4256 (class 2606 OID 1140697)
-- Name: pk_dimension_domain; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dimension_domain
    ADD CONSTRAINT pk_dimension_domain PRIMARY KEY (domain_id, dimension_id);


--
-- TOC entry 4258 (class 2606 OID 1140699)
-- Name: pk_domain; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY domain
    ADD CONSTRAINT pk_domain PRIMARY KEY (domain_id);


--
-- TOC entry 4342 (class 2606 OID 1143795)
-- Name: pk_indexing_type; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY indexing_type
    ADD CONSTRAINT pk_indexing_type PRIMARY KEY (indexing_type_id);


--
-- TOC entry 4283 (class 2606 OID 1140701)
-- Name: pk_instrument; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY instrument
    ADD CONSTRAINT pk_instrument PRIMARY KEY (instrument_type_id, instrument_id);


--
-- TOC entry 4287 (class 2606 OID 1140703)
-- Name: pk_instrument_type; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY instrument_type
    ADD CONSTRAINT pk_instrument_type PRIMARY KEY (instrument_type_id);


--
-- TOC entry 4294 (class 2606 OID 1140707)
-- Name: pk_measurement_metatype; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY measurement_metatype
    ADD CONSTRAINT pk_measurement_metatype PRIMARY KEY (measurement_metatype_id);


--
-- TOC entry 4292 (class 2606 OID 1140705)
-- Name: pk_measurement_type; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY measurement_type
    ADD CONSTRAINT pk_measurement_type PRIMARY KEY (measurement_metatype_id, measurement_type_id);


--
-- TOC entry 4300 (class 2606 OID 1140709)
-- Name: pk_ndarray; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY ndarray
    ADD CONSTRAINT pk_ndarray PRIMARY KEY (ndarray_type_id, ndarray_id, ndarray_version);


--
-- TOC entry 4306 (class 2606 OID 1140711)
-- Name: pk_ndarray_dataset; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY ndarray_dataset
    ADD CONSTRAINT pk_ndarray_dataset PRIMARY KEY (ndarray_type_id, ndarray_id, ndarray_version, dataset_type_id, dataset_id);


--
-- TOC entry 4310 (class 2606 OID 1140713)
-- Name: pk_ndarray_dimension; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY ndarray_dimension
    ADD CONSTRAINT pk_ndarray_dimension PRIMARY KEY (ndarray_type_id, ndarray_id, ndarray_version, domain_id, dimension_id);


--
-- TOC entry 4336 (class 2606 OID 1143742)
-- Name: pk_ndarray_footprint; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY ndarray_footprint
    ADD CONSTRAINT pk_ndarray_footprint PRIMARY KEY (ndarray_type_id, ndarray_footprint_id);


--
-- TOC entry 4340 (class 2606 OID 1143762)
-- Name: pk_ndarray_footprint_dimension; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY ndarray_footprint_dimension
    ADD CONSTRAINT pk_ndarray_footprint_dimension PRIMARY KEY (ndarray_type_id, ndarray_footprint_id, domain_id, dimension_id);


--
-- TOC entry 4262 (class 2606 OID 1140715)
-- Name: pk_ndarray_type; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY ndarray_type
    ADD CONSTRAINT pk_ndarray_type PRIMARY KEY (ndarray_type_id);


--
-- TOC entry 4269 (class 2606 OID 1140717)
-- Name: pk_ndarray_type_dimension; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY ndarray_type_dimension
    ADD CONSTRAINT pk_ndarray_type_dimension PRIMARY KEY (ndarray_type_id, domain_id, dimension_id);


--
-- TOC entry 4351 (class 2606 OID 1143885)
-- Name: pk_ndarray_type_dimension_property; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY ndarray_type_dimension_property
    ADD CONSTRAINT pk_ndarray_type_dimension_property PRIMARY KEY (ndarray_type_id, domain_id, dimension_id, property_id);


--
-- TOC entry 4315 (class 2606 OID 1140721)
-- Name: pk_ndarray_type_measurement_type; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY ndarray_type_measurement_type
    ADD CONSTRAINT pk_ndarray_type_measurement_type PRIMARY KEY (ndarray_type_id, measurement_metatype_id, measurement_type_id);


--
-- TOC entry 4321 (class 2606 OID 1140723)
-- Name: pk_observation; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY observation
    ADD CONSTRAINT pk_observation PRIMARY KEY (observation_type_id, observation_id);


--
-- TOC entry 4323 (class 2606 OID 1140725)
-- Name: pk_observation_type; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY observation_type
    ADD CONSTRAINT pk_observation_type PRIMARY KEY (observation_type_id);


--
-- TOC entry 4328 (class 2606 OID 1140727)
-- Name: pk_platform; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY platform
    ADD CONSTRAINT pk_platform PRIMARY KEY (platform_type_id, platform_id);


--
-- TOC entry 4332 (class 2606 OID 1140729)
-- Name: pk_platform_type; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY platform_type
    ADD CONSTRAINT pk_platform_type PRIMARY KEY (platform_type_id);


--
-- TOC entry 4273 (class 2606 OID 1140731)
-- Name: pk_reference_system; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY reference_system
    ADD CONSTRAINT pk_reference_system PRIMARY KEY (reference_system_id);


--
-- TOC entry 4279 (class 2606 OID 1140733)
-- Name: pk_reference_system_indexing; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY reference_system_indexing
    ADD CONSTRAINT pk_reference_system_indexing PRIMARY KEY (reference_system_id, array_index);


--
-- TOC entry 4353 (class 2606 OID 1143881)
-- Name: property_pkey; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY property
    ADD CONSTRAINT property_pkey PRIMARY KEY (property_id);


--
-- TOC entry 4226 (class 2606 OID 1140735)
-- Name: uq_dataset_type_dataset_type_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dataset_type
    ADD CONSTRAINT uq_dataset_type_dataset_type_name UNIQUE (dataset_type_name);


--
-- TOC entry 4242 (class 2606 OID 1140737)
-- Name: uq_dataset_type_measurement_type_ndarray_type_id_measurement_ty; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dataset_type_measurement_type
    ADD CONSTRAINT uq_dataset_type_measurement_type_ndarray_type_id_measurement_ty UNIQUE (dataset_type_id, measurement_type_index);


--
-- TOC entry 4246 (class 2606 OID 1140739)
-- Name: uq_datatype_datatype_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY datatype
    ADD CONSTRAINT uq_datatype_datatype_name UNIQUE (datatype_name);


--
-- TOC entry 4250 (class 2606 OID 1140741)
-- Name: uq_dimension_dimension_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dimension
    ADD CONSTRAINT uq_dimension_dimension_name UNIQUE (dimension_name);


--
-- TOC entry 4252 (class 2606 OID 1140743)
-- Name: uq_dimension_dimension_tag; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dimension
    ADD CONSTRAINT uq_dimension_dimension_tag UNIQUE (dimension_tag);


--
-- TOC entry 4260 (class 2606 OID 1140745)
-- Name: uq_domain_domain_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY domain
    ADD CONSTRAINT uq_domain_domain_name UNIQUE (domain_name);


--
-- TOC entry 4344 (class 2606 OID 1143797)
-- Name: uq_indexing_type_indexing_type_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY indexing_type
    ADD CONSTRAINT uq_indexing_type_indexing_type_name UNIQUE (indexing_type_name);


--
-- TOC entry 4285 (class 2606 OID 1140747)
-- Name: uq_instrument_instrument_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY instrument
    ADD CONSTRAINT uq_instrument_instrument_name UNIQUE (instrument_name);


--
-- TOC entry 4289 (class 2606 OID 1140749)
-- Name: uq_instrument_type_instrument_type_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY instrument_type
    ADD CONSTRAINT uq_instrument_type_instrument_type_name UNIQUE (instrument_type_name);


--
-- TOC entry 4296 (class 2606 OID 1140751)
-- Name: uq_measurement_metatype_measurement_metatype_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY measurement_metatype
    ADD CONSTRAINT uq_measurement_metatype_measurement_metatype_name UNIQUE (measurement_metatype_name);


--
-- TOC entry 4302 (class 2606 OID 1140753)
-- Name: uq_ndarray_ndarray_location; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY ndarray
    ADD CONSTRAINT uq_ndarray_ndarray_location UNIQUE (ndarray_location);


--
-- TOC entry 4271 (class 2606 OID 1143831)
-- Name: uq_ndarray_type_dimension_ndarray_type_dimension; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY ndarray_type_dimension
    ADD CONSTRAINT uq_ndarray_type_dimension_ndarray_type_dimension UNIQUE (ndarray_type_id, dimension_id);


--
-- TOC entry 4594 (class 0 OID 0)
-- Dependencies: 4271
-- Name: CONSTRAINT uq_ndarray_type_dimension_ndarray_type_dimension ON ndarray_type_dimension; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON CONSTRAINT uq_ndarray_type_dimension_ndarray_type_dimension ON ndarray_type_dimension IS 'Unique constraint to ensure each dimension is only represented once in each ndarray_type';


--
-- TOC entry 4317 (class 2606 OID 1140757)
-- Name: uq_ndarray_type_measurement_type_ndarray_type_id_measurement_ty; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY ndarray_type_measurement_type
    ADD CONSTRAINT uq_ndarray_type_measurement_type_ndarray_type_id_measurement_ty UNIQUE (ndarray_type_id, measurement_type_index);


--
-- TOC entry 4264 (class 2606 OID 1140755)
-- Name: uq_ndarray_type_ndarray_type_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY ndarray_type
    ADD CONSTRAINT uq_ndarray_type_ndarray_type_name UNIQUE (ndarray_type_name);


--
-- TOC entry 4325 (class 2606 OID 1140759)
-- Name: uq_observation_type_observation_type_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY observation_type
    ADD CONSTRAINT uq_observation_type_observation_type_name UNIQUE (observation_type_name);


--
-- TOC entry 4330 (class 2606 OID 1140761)
-- Name: uq_platform_platform_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY platform
    ADD CONSTRAINT uq_platform_platform_name UNIQUE (platform_name);


--
-- TOC entry 4334 (class 2606 OID 1140763)
-- Name: uq_platform_type_platform_type_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY platform_type
    ADD CONSTRAINT uq_platform_type_platform_type_name UNIQUE (platform_type_name);


--
-- TOC entry 4355 (class 2606 OID 1143883)
-- Name: uq_property_property_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY property
    ADD CONSTRAINT uq_property_property_name UNIQUE (property_name);


--
-- TOC entry 4275 (class 2606 OID 1140765)
-- Name: uq_reference_system_reference_system_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY reference_system
    ADD CONSTRAINT uq_reference_system_reference_system_name UNIQUE (reference_system_name);


SET search_path = earth_observation, pg_catalog;

--
-- TOC entry 4345 (class 1259 OID 1143827)
-- Name: fki_spectral_parameters_measurement_type; Type: INDEX; Schema: earth_observation; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_spectral_parameters_measurement_type ON spectral_parameters USING btree (measurement_metatype_id, measurement_type_id);


SET search_path = public, pg_catalog;

--
-- TOC entry 4212 (class 1259 OID 1140766)
-- Name: fki_dataset_dataset_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_dataset_type ON dataset USING btree (dataset_type_id);


--
-- TOC entry 4216 (class 1259 OID 1140767)
-- Name: fki_dataset_dimension_dataset; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_dimension_dataset ON dataset_dimension USING btree (dataset_type_id, dataset_id);


--
-- TOC entry 4217 (class 1259 OID 1140768)
-- Name: fki_dataset_dimension_dataset_type_dimension; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_dimension_dataset_type_dimension ON dataset_dimension USING btree (dataset_type_id, domain_id, dimension_id);


--
-- TOC entry 4220 (class 1259 OID 1140769)
-- Name: fki_dataset_metadata_dataset; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_metadata_dataset ON dataset_metadata USING btree (dataset_type_id, dataset_id);


--
-- TOC entry 4213 (class 1259 OID 1140770)
-- Name: fki_dataset_observation; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_observation ON dataset USING btree (observation_type_id, observation_id);


--
-- TOC entry 4227 (class 1259 OID 1140771)
-- Name: fki_dataset_type_dimension_dataset_type_domain; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_type_dimension_dataset_type_domain ON dataset_type_dimension USING btree (dataset_type_id, domain_id);


--
-- TOC entry 4228 (class 1259 OID 1140772)
-- Name: fki_dataset_type_dimension_dimension_domain; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_type_dimension_dimension_domain ON dataset_type_dimension USING btree (domain_id, dimension_id);


--
-- TOC entry 4231 (class 1259 OID 1140773)
-- Name: fki_dataset_type_domain_dataset_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_type_domain_dataset_type ON dataset_type_domain USING btree (dataset_type_id);


--
-- TOC entry 4232 (class 1259 OID 1140774)
-- Name: fki_dataset_type_domain_domain; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_type_domain_domain ON dataset_type_domain USING btree (domain_id);


--
-- TOC entry 4233 (class 1259 OID 1140775)
-- Name: fki_dataset_type_domain_reference_system; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_type_domain_reference_system ON dataset_type_domain USING btree (reference_system_id);


--
-- TOC entry 4236 (class 1259 OID 1140778)
-- Name: fki_dataset_type_measurement_metatype_datatype; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_type_measurement_metatype_datatype ON dataset_type_measurement_type USING btree (datatype_id);


--
-- TOC entry 4237 (class 1259 OID 1140776)
-- Name: fki_dataset_type_measurement_type_dataset_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_type_measurement_type_dataset_type ON dataset_type_measurement_type USING btree (dataset_type_id);


--
-- TOC entry 4238 (class 1259 OID 1140777)
-- Name: fki_dataset_type_measurement_type_measurement_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_type_measurement_type_measurement_type ON dataset_type_measurement_type USING btree (measurement_metatype_id, measurement_type_id);


--
-- TOC entry 4253 (class 1259 OID 1140779)
-- Name: fki_dimension_domain_dimension; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dimension_domain_dimension ON dimension_domain USING btree (dimension_id);


--
-- TOC entry 4254 (class 1259 OID 1140780)
-- Name: fki_dimension_domain_domain; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dimension_domain_domain ON dimension_domain USING btree (domain_id);


--
-- TOC entry 4280 (class 1259 OID 1140781)
-- Name: fki_instrument_instrument_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_instrument_instrument_type ON instrument USING btree (instrument_type_id);


--
-- TOC entry 4281 (class 1259 OID 1140782)
-- Name: fki_instrument_platform; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_instrument_platform ON instrument USING btree (platform_type_id, platform_id);


--
-- TOC entry 4290 (class 1259 OID 1140783)
-- Name: fki_measurement_type_measurement_metatype; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_measurement_type_measurement_metatype ON measurement_type USING btree (measurement_metatype_id);


--
-- TOC entry 4303 (class 1259 OID 1140784)
-- Name: fki_ndarray_dataset_dataset; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_ndarray_dataset_dataset ON ndarray_dataset USING btree (dataset_type_id, dataset_id);


--
-- TOC entry 4304 (class 1259 OID 1140785)
-- Name: fki_ndarray_dataset_ndarray; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_ndarray_dataset_ndarray ON ndarray_dataset USING btree (ndarray_type_id, ndarray_id, ndarray_version);


--
-- TOC entry 4307 (class 1259 OID 1140786)
-- Name: fki_ndarray_dimension_ndarray; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_ndarray_dimension_ndarray ON ndarray_dimension USING btree (ndarray_type_id, ndarray_id, ndarray_version);


--
-- TOC entry 4308 (class 1259 OID 1140787)
-- Name: fki_ndarray_dimension_ndarray_type_dimension; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_ndarray_dimension_ndarray_type_dimension ON ndarray_dimension USING btree (ndarray_type_id, domain_id, dimension_id);


--
-- TOC entry 4337 (class 1259 OID 1143773)
-- Name: fki_ndarray_footprint_dimension_ndarray; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_ndarray_footprint_dimension_ndarray ON ndarray_footprint_dimension USING btree (ndarray_type_id, ndarray_footprint_id);


--
-- TOC entry 4338 (class 1259 OID 1143774)
-- Name: fki_ndarray_footprint_dimension_ndarray_type_dimension; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_ndarray_footprint_dimension_ndarray_type_dimension ON ndarray_footprint_dimension USING btree (ndarray_type_id, domain_id, dimension_id);


--
-- TOC entry 4297 (class 1259 OID 1143748)
-- Name: fki_ndarray_footprint_ndarray_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_ndarray_footprint_ndarray_type ON ndarray USING btree (ndarray_type_id);


--
-- TOC entry 4298 (class 1259 OID 1140788)
-- Name: fki_ndarray_ndarray_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_ndarray_ndarray_type ON ndarray USING btree (ndarray_type_id, ndarray_type_id);


--
-- TOC entry 4348 (class 1259 OID 1143892)
-- Name: fki_ndarray_type_dimension_attribute_ndarray_type_dimension; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_ndarray_type_dimension_attribute_ndarray_type_dimension ON ndarray_type_dimension_property USING btree (ndarray_type_id, domain_id, dimension_id);


--
-- TOC entry 4349 (class 1259 OID 1143891)
-- Name: fki_ndarray_type_dimension_attribute_property; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_ndarray_type_dimension_attribute_property ON ndarray_type_dimension_property USING btree (property_id);


--
-- TOC entry 4265 (class 1259 OID 1140789)
-- Name: fki_ndarray_type_dimension_dimension_domain; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_ndarray_type_dimension_dimension_domain ON ndarray_type_dimension USING btree (domain_id, dimension_id);


--
-- TOC entry 4266 (class 1259 OID 1143803)
-- Name: fki_ndarray_type_dimension_indexing_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_ndarray_type_dimension_indexing_type ON ndarray_type_dimension USING btree (indexing_type_id);


--
-- TOC entry 4267 (class 1259 OID 1140790)
-- Name: fki_ndarray_type_dimension_ndarray_type_id_fkey; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_ndarray_type_dimension_ndarray_type_id_fkey ON ndarray_type_dimension USING btree (ndarray_type_id, domain_id);


--
-- TOC entry 4311 (class 1259 OID 1140794)
-- Name: fki_ndarray_type_masurement_type_datatype; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_ndarray_type_masurement_type_datatype ON ndarray_type_measurement_type USING btree (datatype_id);


--
-- TOC entry 4312 (class 1259 OID 1140795)
-- Name: fki_ndarray_type_measurement_type_measurement_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_ndarray_type_measurement_type_measurement_type ON ndarray_type_measurement_type USING btree (measurement_metatype_id, measurement_type_id);


--
-- TOC entry 4313 (class 1259 OID 1140796)
-- Name: fki_ndarray_type_measurement_type_ndarray_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_ndarray_type_measurement_type_ndarray_type ON ndarray_type_measurement_type USING btree (ndarray_type_id);


--
-- TOC entry 4318 (class 1259 OID 1140797)
-- Name: fki_observation_instrument; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_observation_instrument ON observation USING btree (instrument_type_id, instrument_id);


--
-- TOC entry 4319 (class 1259 OID 1140798)
-- Name: fki_observation_observation_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_observation_observation_type ON observation USING btree (observation_type_id);


--
-- TOC entry 4326 (class 1259 OID 1140799)
-- Name: fki_platform_platform_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_platform_platform_type ON platform USING btree (platform_type_id);


--
-- TOC entry 4276 (class 1259 OID 1140800)
-- Name: fki_reference_system_indexing_measurement_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_reference_system_indexing_measurement_type ON reference_system_indexing USING btree (measurement_metatype_id, measurement_type_id);


--
-- TOC entry 4277 (class 1259 OID 1140801)
-- Name: fki_reference_system_indexing_reference_system; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_reference_system_indexing_reference_system ON reference_system_indexing USING btree (reference_system_id);


SET search_path = earth_observation, pg_catalog;

--
-- TOC entry 4393 (class 2606 OID 1143822)
-- Name: fk_spectral_parameters_measurement_metatype; Type: FK CONSTRAINT; Schema: earth_observation; Owner: cube_admin
--

ALTER TABLE ONLY spectral_parameters
    ADD CONSTRAINT fk_spectral_parameters_measurement_metatype FOREIGN KEY (measurement_metatype_id, measurement_type_id) REFERENCES public.measurement_type(measurement_metatype_id, measurement_type_id);


SET search_path = public, pg_catalog;

--
-- TOC entry 4356 (class 2606 OID 1140802)
-- Name: fk_dataset_dataset_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset
    ADD CONSTRAINT fk_dataset_dataset_type FOREIGN KEY (dataset_type_id) REFERENCES dataset_type(dataset_type_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4358 (class 2606 OID 1140807)
-- Name: fk_dataset_dimension_dataset; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset_dimension
    ADD CONSTRAINT fk_dataset_dimension_dataset FOREIGN KEY (dataset_type_id, dataset_id) REFERENCES dataset(dataset_type_id, dataset_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4359 (class 2606 OID 1140812)
-- Name: fk_dataset_dimension_dataset_type_dimension; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset_dimension
    ADD CONSTRAINT fk_dataset_dimension_dataset_type_dimension FOREIGN KEY (dataset_type_id, domain_id, dimension_id) REFERENCES dataset_type_dimension(dataset_type_id, domain_id, dimension_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4360 (class 2606 OID 1140817)
-- Name: fk_dataset_metadata_dataset; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset_metadata
    ADD CONSTRAINT fk_dataset_metadata_dataset FOREIGN KEY (dataset_type_id, dataset_id) REFERENCES dataset(dataset_type_id, dataset_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4357 (class 2606 OID 1140822)
-- Name: fk_dataset_observation; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset
    ADD CONSTRAINT fk_dataset_observation FOREIGN KEY (observation_type_id, observation_id) REFERENCES observation(observation_type_id, observation_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4361 (class 2606 OID 1140827)
-- Name: fk_dataset_type_dimension_dataset_type_domain; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset_type_dimension
    ADD CONSTRAINT fk_dataset_type_dimension_dataset_type_domain FOREIGN KEY (dataset_type_id, domain_id) REFERENCES dataset_type_domain(dataset_type_id, domain_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4362 (class 2606 OID 1140832)
-- Name: fk_dataset_type_dimension_dimension_domain; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset_type_dimension
    ADD CONSTRAINT fk_dataset_type_dimension_dimension_domain FOREIGN KEY (domain_id, dimension_id) REFERENCES dimension_domain(domain_id, dimension_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4363 (class 2606 OID 1140837)
-- Name: fk_dataset_type_domain_dataset_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset_type_domain
    ADD CONSTRAINT fk_dataset_type_domain_dataset_type FOREIGN KEY (dataset_type_id) REFERENCES dataset_type(dataset_type_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4364 (class 2606 OID 1140842)
-- Name: fk_dataset_type_domain_domain; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset_type_domain
    ADD CONSTRAINT fk_dataset_type_domain_domain FOREIGN KEY (domain_id) REFERENCES domain(domain_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4365 (class 2606 OID 1140847)
-- Name: fk_dataset_type_domain_reference_system; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset_type_domain
    ADD CONSTRAINT fk_dataset_type_domain_reference_system FOREIGN KEY (reference_system_id) REFERENCES reference_system(reference_system_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4366 (class 2606 OID 1140852)
-- Name: fk_dataset_type_measurement_type_dataset_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset_type_measurement_type
    ADD CONSTRAINT fk_dataset_type_measurement_type_dataset_type FOREIGN KEY (dataset_type_id) REFERENCES dataset_type(dataset_type_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4367 (class 2606 OID 1140857)
-- Name: fk_dataset_type_measurement_type_datatype; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset_type_measurement_type
    ADD CONSTRAINT fk_dataset_type_measurement_type_datatype FOREIGN KEY (datatype_id) REFERENCES datatype(datatype_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4368 (class 2606 OID 1140862)
-- Name: fk_dataset_type_measurement_type_measurement_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset_type_measurement_type
    ADD CONSTRAINT fk_dataset_type_measurement_type_measurement_type FOREIGN KEY (measurement_metatype_id, measurement_type_id) REFERENCES measurement_type(measurement_metatype_id, measurement_type_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4369 (class 2606 OID 1140867)
-- Name: fk_dimension_domain_dimension; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dimension_domain
    ADD CONSTRAINT fk_dimension_domain_dimension FOREIGN KEY (dimension_id) REFERENCES dimension(dimension_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4370 (class 2606 OID 1140872)
-- Name: fk_dimension_domain_domain; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dimension_domain
    ADD CONSTRAINT fk_dimension_domain_domain FOREIGN KEY (domain_id) REFERENCES domain(domain_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4376 (class 2606 OID 1140877)
-- Name: fk_instrument_instrument_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY instrument
    ADD CONSTRAINT fk_instrument_instrument_type FOREIGN KEY (instrument_type_id) REFERENCES instrument_type(instrument_type_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4377 (class 2606 OID 1140882)
-- Name: fk_instrument_platform; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY instrument
    ADD CONSTRAINT fk_instrument_platform FOREIGN KEY (platform_type_id, platform_id) REFERENCES platform(platform_type_id, platform_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4378 (class 2606 OID 1140887)
-- Name: fk_measurement_type_measurement_metatype; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY measurement_type
    ADD CONSTRAINT fk_measurement_type_measurement_metatype FOREIGN KEY (measurement_metatype_id) REFERENCES measurement_metatype(measurement_metatype_id);


--
-- TOC entry 4380 (class 2606 OID 1140892)
-- Name: fk_ndarray_dataset_dataset; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY ndarray_dataset
    ADD CONSTRAINT fk_ndarray_dataset_dataset FOREIGN KEY (dataset_type_id, dataset_id) REFERENCES dataset(dataset_type_id, dataset_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4381 (class 2606 OID 1140897)
-- Name: fk_ndarray_dataset_ndarray; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY ndarray_dataset
    ADD CONSTRAINT fk_ndarray_dataset_ndarray FOREIGN KEY (ndarray_type_id, ndarray_id, ndarray_version) REFERENCES ndarray(ndarray_type_id, ndarray_id, ndarray_version) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4382 (class 2606 OID 1140902)
-- Name: fk_ndarray_dimension_ndarray; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY ndarray_dimension
    ADD CONSTRAINT fk_ndarray_dimension_ndarray FOREIGN KEY (ndarray_type_id, ndarray_id, ndarray_version) REFERENCES ndarray(ndarray_type_id, ndarray_id, ndarray_version) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4383 (class 2606 OID 1140907)
-- Name: fk_ndarray_dimension_ndarray_type_dimension; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY ndarray_dimension
    ADD CONSTRAINT fk_ndarray_dimension_ndarray_type_dimension FOREIGN KEY (ndarray_type_id, domain_id, dimension_id) REFERENCES ndarray_type_dimension(ndarray_type_id, domain_id, dimension_id);


--
-- TOC entry 4391 (class 2606 OID 1143763)
-- Name: fk_ndarray_footprint_dimension_ndarray_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY ndarray_footprint_dimension
    ADD CONSTRAINT fk_ndarray_footprint_dimension_ndarray_type FOREIGN KEY (ndarray_type_id, ndarray_footprint_id) REFERENCES ndarray_footprint(ndarray_type_id, ndarray_footprint_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4392 (class 2606 OID 1143768)
-- Name: fk_ndarray_footprint_dimension_ndarray_type_dimension; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY ndarray_footprint_dimension
    ADD CONSTRAINT fk_ndarray_footprint_dimension_ndarray_type_dimension FOREIGN KEY (ndarray_type_id, domain_id, dimension_id) REFERENCES ndarray_type_dimension(ndarray_type_id, domain_id, dimension_id);


--
-- TOC entry 4390 (class 2606 OID 1143743)
-- Name: fk_ndarray_footprint_ndarray_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY ndarray_footprint
    ADD CONSTRAINT fk_ndarray_footprint_ndarray_type FOREIGN KEY (ndarray_type_id) REFERENCES ndarray_type(ndarray_type_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4379 (class 2606 OID 1140912)
-- Name: fk_ndarray_ndarray_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY ndarray
    ADD CONSTRAINT fk_ndarray_ndarray_type FOREIGN KEY (ndarray_type_id) REFERENCES ndarray_type(ndarray_type_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4394 (class 2606 OID 1143856)
-- Name: fk_ndarray_type_dimension_attribute_ndarray_type_dimension; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY ndarray_type_dimension_property
    ADD CONSTRAINT fk_ndarray_type_dimension_attribute_ndarray_type_dimension FOREIGN KEY (ndarray_type_id, domain_id, dimension_id) REFERENCES ndarray_type_dimension(ndarray_type_id, domain_id, dimension_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4371 (class 2606 OID 1140917)
-- Name: fk_ndarray_type_dimension_dimension_domain; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY ndarray_type_dimension
    ADD CONSTRAINT fk_ndarray_type_dimension_dimension_domain FOREIGN KEY (domain_id, dimension_id) REFERENCES dimension_domain(domain_id, dimension_id) ON UPDATE CASCADE;


--
-- TOC entry 4372 (class 2606 OID 1143798)
-- Name: fk_ndarray_type_dimension_indexing_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY ndarray_type_dimension
    ADD CONSTRAINT fk_ndarray_type_dimension_indexing_type FOREIGN KEY (indexing_type_id) REFERENCES indexing_type(indexing_type_id);


--
-- TOC entry 4395 (class 2606 OID 1143886)
-- Name: fk_ndarray_type_dimension_property_property; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY ndarray_type_dimension_property
    ADD CONSTRAINT fk_ndarray_type_dimension_property_property FOREIGN KEY (property_id) REFERENCES property(property_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4373 (class 2606 OID 1143832)
-- Name: fk_ndarray_type_dimension_reference_system; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY ndarray_type_dimension
    ADD CONSTRAINT fk_ndarray_type_dimension_reference_system FOREIGN KEY (reference_system_id) REFERENCES reference_system(reference_system_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4384 (class 2606 OID 1140942)
-- Name: fk_ndarray_type_measurement_type_datatype; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY ndarray_type_measurement_type
    ADD CONSTRAINT fk_ndarray_type_measurement_type_datatype FOREIGN KEY (datatype_id) REFERENCES datatype(datatype_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4385 (class 2606 OID 1140947)
-- Name: fk_ndarray_type_measurement_type_measurement_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY ndarray_type_measurement_type
    ADD CONSTRAINT fk_ndarray_type_measurement_type_measurement_type FOREIGN KEY (measurement_metatype_id, measurement_type_id) REFERENCES measurement_type(measurement_metatype_id, measurement_type_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4386 (class 2606 OID 1140952)
-- Name: fk_ndarray_type_measurement_type_ndarray_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY ndarray_type_measurement_type
    ADD CONSTRAINT fk_ndarray_type_measurement_type_ndarray_type FOREIGN KEY (ndarray_type_id) REFERENCES ndarray_type(ndarray_type_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4387 (class 2606 OID 1140957)
-- Name: fk_observation_instrument; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY observation
    ADD CONSTRAINT fk_observation_instrument FOREIGN KEY (instrument_type_id, instrument_id) REFERENCES instrument(instrument_type_id, instrument_id);


--
-- TOC entry 4388 (class 2606 OID 1140962)
-- Name: fk_observation_observation_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY observation
    ADD CONSTRAINT fk_observation_observation_type FOREIGN KEY (observation_type_id) REFERENCES observation_type(observation_type_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4389 (class 2606 OID 1140967)
-- Name: fk_platform_platform_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY platform
    ADD CONSTRAINT fk_platform_platform_type FOREIGN KEY (platform_type_id) REFERENCES platform_type(platform_type_id);


--
-- TOC entry 4374 (class 2606 OID 1140972)
-- Name: fk_reference_system_indexing_measurement_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY reference_system_indexing
    ADD CONSTRAINT fk_reference_system_indexing_measurement_type FOREIGN KEY (measurement_metatype_id, measurement_type_id) REFERENCES measurement_type(measurement_metatype_id, measurement_type_id);


--
-- TOC entry 4375 (class 2606 OID 1140977)
-- Name: fk_reference_system_indexing_reference_system; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY reference_system_indexing
    ADD CONSTRAINT fk_reference_system_indexing_reference_system FOREIGN KEY (reference_system_id) REFERENCES reference_system(reference_system_id);


--
-- TOC entry 4546 (class 0 OID 1143964)
-- Dependencies: 228 4550
-- Name: ndarray_type_dimension_view; Type: MATERIALIZED VIEW DATA; Schema: public; Owner: cube_admin
--

REFRESH MATERIALIZED VIEW ndarray_type_dimension_view;


--
-- TOC entry 4547 (class 0 OID 1143972)
-- Dependencies: 229 4546 4550
-- Name: dimension_indices_view; Type: MATERIALIZED VIEW DATA; Schema: public; Owner: cube_admin
--

REFRESH MATERIALIZED VIEW dimension_indices_view;


--
-- TOC entry 4548 (class 0 OID 1143976)
-- Dependencies: 230 4546 4550
-- Name: dimension_properties_view; Type: MATERIALIZED VIEW DATA; Schema: public; Owner: cube_admin
--

REFRESH MATERIALIZED VIEW dimension_properties_view;


--
-- TOC entry 4556 (class 0 OID 0)
-- Dependencies: 6
-- Name: public; Type: ACL; Schema: -; Owner: postgres
--

REVOKE ALL ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM postgres;
GRANT ALL ON SCHEMA public TO postgres;
GRANT ALL ON SCHEMA public TO cube_admin;
GRANT ALL ON SCHEMA public TO PUBLIC;


--
-- TOC entry 4558 (class 0 OID 0)
-- Dependencies: 7
-- Name: ztmp; Type: ACL; Schema: -; Owner: postgres
--

REVOKE ALL ON SCHEMA ztmp FROM PUBLIC;
REVOKE ALL ON SCHEMA ztmp FROM postgres;
GRANT ALL ON SCHEMA ztmp TO postgres;
GRANT ALL ON SCHEMA ztmp TO PUBLIC;


-- Completed on 2015-03-19 15:49:11 AEDT

--
-- PostgreSQL database dump complete
--

