--
-- PostgreSQL database dump
--

-- Dumped from database version 9.3.6
-- Dumped by pg_dump version 9.3.6
-- Started on 2015-04-05 17:09:42 AEST

SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;

--
-- TOC entry 8 (class 2615 OID 1955144)
-- Name: earth_observation; Type: SCHEMA; Schema: -; Owner: cube_admin
--

CREATE SCHEMA earth_observation;


ALTER SCHEMA earth_observation OWNER TO cube_admin;

--
-- TOC entry 6 (class 2615 OID 994234)
-- Name: topology; Type: SCHEMA; Schema: -; Owner: cube_admin
--

CREATE SCHEMA topology;


ALTER SCHEMA topology OWNER TO cube_admin;

--
-- TOC entry 10 (class 2615 OID 1955145)
-- Name: ztmp; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA ztmp;


ALTER SCHEMA ztmp OWNER TO postgres;

--
-- TOC entry 3931 (class 0 OID 0)
-- Dependencies: 10
-- Name: SCHEMA ztmp; Type: COMMENT; Schema: -; Owner: postgres
--

COMMENT ON SCHEMA ztmp IS 'Temporary schema';


SET search_path = public, pg_catalog;

CREATE EXTENSION adminpack
  SCHEMA pg_catalog
  VERSION "1.0";

CREATE EXTENSION postgis
  SCHEMA public
  VERSION "2.1.3";

CREATE EXTENSION postgis_topology
  SCHEMA topology
  VERSION "2.1.3";

--
-- TOC entry 1746 (class 1247 OID 1955148)
-- Name: attribute_value_type; Type: TYPE; Schema: public; Owner: cube_admin
--

CREATE TYPE attribute_value_type AS (
	attribute_type_id bigint,
	attribute_id bigint,
	attribute_value_id bigint
);


ALTER TYPE public.attribute_value_type OWNER TO cube_admin;

--
-- TOC entry 1749 (class 1247 OID 1955151)
-- Name: category_id_level_type; Type: TYPE; Schema: public; Owner: cube_admin
--

CREATE TYPE category_id_level_type AS (
	category_type_id bigint,
	category_id bigint,
	level integer
);


ALTER TYPE public.category_id_level_type OWNER TO cube_admin;

--
-- TOC entry 1752 (class 1247 OID 1955154)
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
-- TOC entry 205 (class 1259 OID 1955155)
-- Name: spectral_parameters; Type: TABLE; Schema: earth_observation; Owner: cube_admin; Tablespace: 
--

CREATE TABLE spectral_parameters (
    measurement_metatype_id bigint NOT NULL,
    measurement_type_id bigint NOT NULL
);


ALTER TABLE earth_observation.spectral_parameters OWNER TO cube_admin;

--
-- TOC entry 3933 (class 0 OID 0)
-- Dependencies: 205
-- Name: TABLE spectral_parameters; Type: COMMENT; Schema: earth_observation; Owner: cube_admin
--

COMMENT ON TABLE spectral_parameters IS 'Configuration: Spectral band parameters';


SET search_path = public, pg_catalog;

--
-- TOC entry 206 (class 1259 OID 1955158)
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
-- TOC entry 3935 (class 0 OID 0)
-- Dependencies: 206
-- Name: TABLE dataset; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE dataset IS 'Data: Source dataset (file) ingested.
An example would be a dataset for a particular NBAR Landsat scene.';


--
-- TOC entry 207 (class 1259 OID 1955161)
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
-- TOC entry 3937 (class 0 OID 0)
-- Dependencies: 207
-- Name: TABLE dataset_dimension; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE dataset_dimension IS 'Data: Dimensional parameters for each source dataset.
Each dataset/dimension will have specific max/min/indexing values showing the range covered by the dataset in that particular dimension.';


--
-- TOC entry 208 (class 1259 OID 1955164)
-- Name: dataset_metadata; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE dataset_metadata (
    dataset_type_id bigint NOT NULL,
    dataset_id bigint NOT NULL,
    metadata_xml xml NOT NULL
);


ALTER TABLE public.dataset_metadata OWNER TO cube_admin;

--
-- TOC entry 3939 (class 0 OID 0)
-- Dependencies: 208
-- Name: TABLE dataset_metadata; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE dataset_metadata IS 'Data: Lookup table for dataset-level metadata (one:one)';


--
-- TOC entry 209 (class 1259 OID 1955170)
-- Name: dataset_type; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE dataset_type (
    dataset_type_id bigint NOT NULL,
    dataset_type_name character varying(254)
);


ALTER TABLE public.dataset_type OWNER TO cube_admin;

--
-- TOC entry 3941 (class 0 OID 0)
-- Dependencies: 209
-- Name: TABLE dataset_type; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE dataset_type IS 'Configuration: Type of source dataset (processing level)';


--
-- TOC entry 210 (class 1259 OID 1955173)
-- Name: dataset_type_dimension; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE dataset_type_dimension (
    dataset_type_id bigint NOT NULL,
    domain_id bigint NOT NULL,
    dimension_id bigint NOT NULL
);


ALTER TABLE public.dataset_type_dimension OWNER TO cube_admin;

--
-- TOC entry 3943 (class 0 OID 0)
-- Dependencies: 210
-- Name: TABLE dataset_type_dimension; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE dataset_type_dimension IS 'Configuration: Association between dataset type and dimensions. Used to define dimensionality of source dataset types';


--
-- TOC entry 211 (class 1259 OID 1955176)
-- Name: dataset_type_domain; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE dataset_type_domain (
    dataset_type_id bigint NOT NULL,
    domain_id bigint NOT NULL,
    reference_system_id bigint
);


ALTER TABLE public.dataset_type_domain OWNER TO cube_admin;

--
-- TOC entry 3945 (class 0 OID 0)
-- Dependencies: 211
-- Name: TABLE dataset_type_domain; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE dataset_type_domain IS 'Configuration: Association between dataset types and domains (many-many).
Used to define which domains cover a given dataset type';


--
-- TOC entry 212 (class 1259 OID 1955179)
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
-- TOC entry 3947 (class 0 OID 0)
-- Dependencies: 212
-- Name: TABLE dataset_type_measurement_type; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE dataset_type_measurement_type IS 'Configuration: Associations between dataset types and measurement types (one-many)
e.g. associations between Landsat 7 NBAR and specific surface-reflectance corrected Landsat 7 bands';


--
-- TOC entry 213 (class 1259 OID 1955182)
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
-- TOC entry 3949 (class 0 OID 0)
-- Dependencies: 213
-- Name: TABLE datatype; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE datatype IS 'Configuration: Lookup table for measurement_type datatypes.';


--
-- TOC entry 214 (class 1259 OID 1955185)
-- Name: dimension; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE dimension (
    dimension_id bigint NOT NULL,
    dimension_name character varying(50) NOT NULL,
    dimension_tag character varying(8) NOT NULL
);


ALTER TABLE public.dimension OWNER TO cube_admin;

--
-- TOC entry 3951 (class 0 OID 0)
-- Dependencies: 214
-- Name: TABLE dimension; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE dimension IS 'Configuration: Dimensions for n-dimensional data structures, e.g. x,y,z,t';


--
-- TOC entry 215 (class 1259 OID 1955188)
-- Name: dimension_domain; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE dimension_domain (
    domain_id bigint NOT NULL,
    dimension_id bigint NOT NULL
);


ALTER TABLE public.dimension_domain OWNER TO cube_admin;

--
-- TOC entry 3953 (class 0 OID 0)
-- Dependencies: 215
-- Name: TABLE dimension_domain; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE dimension_domain IS 'Configuration: Many-many  mapping between dimensions and domains to allow multiple dimensions to be included in multiple domains.
For example, the z dimension could be managed in a Z-spatial domain, or in an XYZ-spatial domain.';


--
-- TOC entry 216 (class 1259 OID 1955191)
-- Name: domain; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE domain (
    domain_id bigint NOT NULL,
    domain_name character varying(16),
    domain_tag character varying(16)
);


ALTER TABLE public.domain OWNER TO cube_admin;

--
-- TOC entry 3955 (class 0 OID 0)
-- Dependencies: 216
-- Name: TABLE domain; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE domain IS 'Configuration: Domain groupings of dimensions (e.g. spectral, spatial XY, spatial XYZ, temporal)';


--
-- TOC entry 217 (class 1259 OID 1955194)
-- Name: indexing_type; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE indexing_type (
    indexing_type_id smallint NOT NULL,
    indexing_type_name character varying(128)
);


ALTER TABLE public.indexing_type OWNER TO cube_admin;

--
-- TOC entry 3957 (class 0 OID 0)
-- Dependencies: 217
-- Name: TABLE indexing_type; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE indexing_type IS 'Configuration: Lookup table to manage what kind of indexing to apply to a given dimension';


--
-- TOC entry 218 (class 1259 OID 1955197)
-- Name: ndarray_type; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE ndarray_type (
    ndarray_type_id bigint NOT NULL,
    ndarray_type_name character varying(254),
    ndarray_type_tag character varying(16)
);


ALTER TABLE public.ndarray_type OWNER TO cube_admin;

--
-- TOC entry 3959 (class 0 OID 0)
-- Dependencies: 218
-- Name: TABLE ndarray_type; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE ndarray_type IS 'Configuration: ndarray parameter lookup table. Used to manage different ndarray_types';


--
-- TOC entry 219 (class 1259 OID 1955200)
-- Name: ndarray_type_dimension; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE ndarray_type_dimension (
    ndarray_type_id bigint NOT NULL,
    domain_id bigint NOT NULL,
    dimension_id bigint NOT NULL,
    creation_order smallint,
    dimension_extent double precision,
    dimension_elements bigint,
    dimension_cache bigint,
    dimension_origin double precision,
    indexing_type_id smallint,
    reference_system_id bigint,
    index_reference_system_id bigint
);


ALTER TABLE public.ndarray_type_dimension OWNER TO cube_admin;

--
-- TOC entry 3961 (class 0 OID 0)
-- Dependencies: 219
-- Name: TABLE ndarray_type_dimension; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE ndarray_type_dimension IS 'Configuration: Association between attribute set and dimensions. Used to define dimensionality of ndarrays using a given attribute set';


--
-- TOC entry 220 (class 1259 OID 1955206)
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
-- TOC entry 3963 (class 0 OID 0)
-- Dependencies: 220
-- Name: TABLE reference_system; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE reference_system IS 'Configuration: Coordinate reference systems for aplication to specific domains.
e.g. EPSG:4326, seconds since 1/1/1970 0:00, etc.';


--
-- TOC entry 258 (class 1259 OID 1960875)
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
    index_reference_system.reference_system_unit AS index_unit,
    ndarray_type_dimension.dimension_elements,
    reference_system.reference_system_unit,
    ndarray_type_dimension.dimension_cache
   FROM (((((((ndarray_type
     JOIN ndarray_type_dimension USING (ndarray_type_id))
     JOIN dimension_domain USING (domain_id, dimension_id))
     JOIN domain USING (domain_id))
     JOIN dimension USING (dimension_id))
     JOIN reference_system USING (reference_system_id))
     JOIN reference_system index_reference_system ON ((ndarray_type_dimension.index_reference_system_id = index_reference_system.reference_system_id)))
     JOIN indexing_type USING (indexing_type_id))
  ORDER BY ndarray_type.ndarray_type_id, ndarray_type_dimension.creation_order
  WITH NO DATA;


ALTER TABLE public.ndarray_type_dimension_view OWNER TO cube_admin;

--
-- TOC entry 221 (class 1259 OID 1955217)
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
-- TOC entry 3966 (class 0 OID 0)
-- Dependencies: 221
-- Name: TABLE reference_system_indexing; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE reference_system_indexing IS 'Configuration: Optional non-linear indexing for dimension in a given domain.
e.g. A spectral dimension containing multple bands needs to be indexed by band number, and each band number can be associated with a given measurement_type.';


--
-- TOC entry 259 (class 1259 OID 1960883)
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
-- TOC entry 222 (class 1259 OID 1955224)
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
-- TOC entry 223 (class 1259 OID 1955227)
-- Name: property; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE property (
    property_id bigint NOT NULL,
    property_name character varying(32),
    datatype_id smallint
);


ALTER TABLE public.property OWNER TO cube_admin;

--
-- TOC entry 260 (class 1259 OID 1960887)
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
-- TOC entry 224 (class 1259 OID 1955235)
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
-- TOC entry 3972 (class 0 OID 0)
-- Dependencies: 224
-- Name: TABLE instrument; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE instrument IS 'Configuration: Instrument used to gather observations.
An example would be the ETM+ sensor on the Landsat 7 platform';


--
-- TOC entry 225 (class 1259 OID 1955238)
-- Name: instrument_type; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE instrument_type (
    instrument_type_id bigint NOT NULL,
    instrument_type_name character varying(128)
);


ALTER TABLE public.instrument_type OWNER TO cube_admin;

--
-- TOC entry 3974 (class 0 OID 0)
-- Dependencies: 225
-- Name: TABLE instrument_type; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE instrument_type IS 'Configuration: Lookup table for instrument category';


--
-- TOC entry 226 (class 1259 OID 1955241)
-- Name: measurement_metatype; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE measurement_metatype (
    measurement_metatype_id bigint NOT NULL,
    measurement_metatype_name character varying(254)
);


ALTER TABLE public.measurement_metatype OWNER TO cube_admin;

--
-- TOC entry 3976 (class 0 OID 0)
-- Dependencies: 226
-- Name: TABLE measurement_metatype; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE measurement_metatype IS 'Configuration: General type of measurement (e.g. spectral band)';


--
-- TOC entry 227 (class 1259 OID 1955244)
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
-- TOC entry 3978 (class 0 OID 0)
-- Dependencies: 227
-- Name: TABLE measurement_type; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE measurement_type IS 'Configuration: Description of measurement(s) held in n-dimensional data structures: e.g. bands';


--
-- TOC entry 228 (class 1259 OID 1955247)
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
-- TOC entry 3980 (class 0 OID 0)
-- Dependencies: 228
-- Name: TABLE ndarray; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE ndarray IS 'Data: n-dimensional data structure instances';


--
-- TOC entry 229 (class 1259 OID 1955250)
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
-- TOC entry 3982 (class 0 OID 0)
-- Dependencies: 229
-- Name: TABLE ndarray_dataset; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE ndarray_dataset IS 'Data: Association between ndarray and dataset instances (many-many)';


--
-- TOC entry 230 (class 1259 OID 1955253)
-- Name: ndarray_dimension; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE ndarray_dimension (
    ndarray_type_id bigint NOT NULL,
    ndarray_id bigint NOT NULL,
    ndarray_version integer NOT NULL,
    domain_id bigint NOT NULL,
    dimension_id bigint NOT NULL,
    ndarray_dimension_index integer NOT NULL,
    ndarray_dimension_min double precision,
    ndarray_dimension_max double precision
);


ALTER TABLE public.ndarray_dimension OWNER TO cube_admin;

--
-- TOC entry 3984 (class 0 OID 0)
-- Dependencies: 230
-- Name: TABLE ndarray_dimension; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE ndarray_dimension IS 'Data: Association between ndarray and dimensions. Used to define attributes for each dimension in ndarray instances';


--
-- TOC entry 231 (class 1259 OID 1955256)
-- Name: ndarray_footprint; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE ndarray_footprint (
    ndarray_type_id bigint NOT NULL,
    ndarray_footprint_id bigint NOT NULL,
    bounding_box geometry
);


ALTER TABLE public.ndarray_footprint OWNER TO cube_admin;

--
-- TOC entry 3986 (class 0 OID 0)
-- Dependencies: 231
-- Name: TABLE ndarray_footprint; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE ndarray_footprint IS 'Data: Spatial footprint for tiles with a given set of indices in the XY dimensions';


--
-- TOC entry 232 (class 1259 OID 1955259)
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
-- TOC entry 3988 (class 0 OID 0)
-- Dependencies: 232
-- Name: TABLE ndarray_footprint_dimension; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE ndarray_footprint_dimension IS 'Data: Association between ndarray footprint and dimensions';


--
-- TOC entry 257 (class 1259 OID 1957097)
-- Name: ndarray_id_seq; Type: SEQUENCE; Schema: public; Owner: cube_admin
--

CREATE SEQUENCE ndarray_id_seq
    START WITH 100
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.ndarray_id_seq OWNER TO cube_admin;

--
-- TOC entry 233 (class 1259 OID 1955262)
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
-- TOC entry 3990 (class 0 OID 0)
-- Dependencies: 233
-- Name: TABLE ndarray_type_measurement_type; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE ndarray_type_measurement_type IS 'Configuration: Associations between n-dimensional data structure types and measurement types (many-many)
e.g. associations between Landsat 7 arrays and Landsat 7 bands if they are stored in separate variables';


--
-- TOC entry 234 (class 1259 OID 1955265)
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
-- TOC entry 3992 (class 0 OID 0)
-- Dependencies: 234
-- Name: TABLE observation; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE observation IS 'Data: Source observation for datasets.
Analagous to old "acquisition" table in AGDC version 0 DB';


--
-- TOC entry 235 (class 1259 OID 1955268)
-- Name: observation_type; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE observation_type (
    observation_type_id bigint NOT NULL,
    observation_type_name character varying(254)
);


ALTER TABLE public.observation_type OWNER TO cube_admin;

--
-- TOC entry 3994 (class 0 OID 0)
-- Dependencies: 235
-- Name: TABLE observation_type; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE observation_type IS 'Configuration: Type of source observation';


--
-- TOC entry 236 (class 1259 OID 1955271)
-- Name: platform; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE platform (
    platform_type_id bigint NOT NULL,
    platform_id bigint NOT NULL,
    platform_name character varying(128)
);


ALTER TABLE public.platform OWNER TO cube_admin;

--
-- TOC entry 3996 (class 0 OID 0)
-- Dependencies: 236
-- Name: TABLE platform; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE platform IS 'Configuration: Platform on which instrument is mounted.
An example would be a specific satellite such as Landsat 7';


--
-- TOC entry 237 (class 1259 OID 1955274)
-- Name: platform_type; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE platform_type (
    platform_type_id bigint NOT NULL,
    platform_type_name character varying(128)
);


ALTER TABLE public.platform_type OWNER TO cube_admin;

--
-- TOC entry 3998 (class 0 OID 0)
-- Dependencies: 237
-- Name: TABLE platform_type; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE platform_type IS 'Configuration: Lookup table for platform category
e.g. Satellite or Ship';


SET search_path = earth_observation, pg_catalog;

--
-- TOC entry 3621 (class 2606 OID 1955278)
-- Name: pk_spectral_parameters; Type: CONSTRAINT; Schema: earth_observation; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY spectral_parameters
    ADD CONSTRAINT pk_spectral_parameters PRIMARY KEY (measurement_metatype_id, measurement_type_id);


SET search_path = public, pg_catalog;

--
-- TOC entry 3625 (class 2606 OID 1955280)
-- Name: pk_dataset; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dataset
    ADD CONSTRAINT pk_dataset PRIMARY KEY (dataset_type_id, dataset_id);


--
-- TOC entry 3629 (class 2606 OID 1955282)
-- Name: pk_dataset_dimension; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dataset_dimension
    ADD CONSTRAINT pk_dataset_dimension PRIMARY KEY (dataset_type_id, dataset_id, domain_id, dimension_id);


--
-- TOC entry 3632 (class 2606 OID 1955284)
-- Name: pk_dataset_metadata; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dataset_metadata
    ADD CONSTRAINT pk_dataset_metadata PRIMARY KEY (dataset_type_id, dataset_id);


--
-- TOC entry 3634 (class 2606 OID 1955286)
-- Name: pk_dataset_type; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dataset_type
    ADD CONSTRAINT pk_dataset_type PRIMARY KEY (dataset_type_id);


--
-- TOC entry 3640 (class 2606 OID 1955288)
-- Name: pk_dataset_type_dimension; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dataset_type_dimension
    ADD CONSTRAINT pk_dataset_type_dimension PRIMARY KEY (dataset_type_id, domain_id, dimension_id);


--
-- TOC entry 3645 (class 2606 OID 1955290)
-- Name: pk_dataset_type_domain; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dataset_type_domain
    ADD CONSTRAINT pk_dataset_type_domain PRIMARY KEY (dataset_type_id, domain_id);


--
-- TOC entry 3650 (class 2606 OID 1955292)
-- Name: pk_dataset_type_measurement_type; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dataset_type_measurement_type
    ADD CONSTRAINT pk_dataset_type_measurement_type PRIMARY KEY (dataset_type_id, measurement_metatype_id, measurement_type_id);


--
-- TOC entry 3654 (class 2606 OID 1955294)
-- Name: pk_datatype; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY datatype
    ADD CONSTRAINT pk_datatype PRIMARY KEY (datatype_id);


--
-- TOC entry 3658 (class 2606 OID 1955296)
-- Name: pk_dimension; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dimension
    ADD CONSTRAINT pk_dimension PRIMARY KEY (dimension_id);


--
-- TOC entry 3666 (class 2606 OID 1955298)
-- Name: pk_dimension_domain; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dimension_domain
    ADD CONSTRAINT pk_dimension_domain PRIMARY KEY (domain_id, dimension_id);


--
-- TOC entry 3668 (class 2606 OID 1955300)
-- Name: pk_domain; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY domain
    ADD CONSTRAINT pk_domain PRIMARY KEY (domain_id);


--
-- TOC entry 3672 (class 2606 OID 1955302)
-- Name: pk_indexing_type; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY indexing_type
    ADD CONSTRAINT pk_indexing_type PRIMARY KEY (indexing_type_id);


--
-- TOC entry 3705 (class 2606 OID 1955304)
-- Name: pk_instrument; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY instrument
    ADD CONSTRAINT pk_instrument PRIMARY KEY (instrument_type_id, instrument_id);


--
-- TOC entry 3709 (class 2606 OID 1955306)
-- Name: pk_instrument_type; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY instrument_type
    ADD CONSTRAINT pk_instrument_type PRIMARY KEY (instrument_type_id);


--
-- TOC entry 3713 (class 2606 OID 1955308)
-- Name: pk_measurement_metatype; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY measurement_metatype
    ADD CONSTRAINT pk_measurement_metatype PRIMARY KEY (measurement_metatype_id);


--
-- TOC entry 3718 (class 2606 OID 1955310)
-- Name: pk_measurement_type; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY measurement_type
    ADD CONSTRAINT pk_measurement_type PRIMARY KEY (measurement_metatype_id, measurement_type_id);


--
-- TOC entry 3722 (class 2606 OID 1955312)
-- Name: pk_ndarray; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY ndarray
    ADD CONSTRAINT pk_ndarray PRIMARY KEY (ndarray_type_id, ndarray_id, ndarray_version);


--
-- TOC entry 3728 (class 2606 OID 1955314)
-- Name: pk_ndarray_dataset; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY ndarray_dataset
    ADD CONSTRAINT pk_ndarray_dataset PRIMARY KEY (ndarray_type_id, ndarray_id, ndarray_version, dataset_type_id, dataset_id);


--
-- TOC entry 3735 (class 2606 OID 1955316)
-- Name: pk_ndarray_dimension; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY ndarray_dimension
    ADD CONSTRAINT pk_ndarray_dimension PRIMARY KEY (ndarray_type_id, ndarray_id, ndarray_version, domain_id, dimension_id);


--
-- TOC entry 3737 (class 2606 OID 1955318)
-- Name: pk_ndarray_footprint; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY ndarray_footprint
    ADD CONSTRAINT pk_ndarray_footprint PRIMARY KEY (ndarray_type_id, ndarray_footprint_id);


--
-- TOC entry 3741 (class 2606 OID 1955320)
-- Name: pk_ndarray_footprint_dimension; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY ndarray_footprint_dimension
    ADD CONSTRAINT pk_ndarray_footprint_dimension PRIMARY KEY (ndarray_type_id, ndarray_footprint_id, domain_id, dimension_id);


--
-- TOC entry 3676 (class 2606 OID 1955322)
-- Name: pk_ndarray_type; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY ndarray_type
    ADD CONSTRAINT pk_ndarray_type PRIMARY KEY (ndarray_type_id);


--
-- TOC entry 3683 (class 2606 OID 1955324)
-- Name: pk_ndarray_type_dimension; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY ndarray_type_dimension
    ADD CONSTRAINT pk_ndarray_type_dimension PRIMARY KEY (ndarray_type_id, domain_id, dimension_id);


--
-- TOC entry 3697 (class 2606 OID 1955326)
-- Name: pk_ndarray_type_dimension_property; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY ndarray_type_dimension_property
    ADD CONSTRAINT pk_ndarray_type_dimension_property PRIMARY KEY (ndarray_type_id, domain_id, dimension_id, property_id);


--
-- TOC entry 3746 (class 2606 OID 1955328)
-- Name: pk_ndarray_type_measurement_type; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY ndarray_type_measurement_type
    ADD CONSTRAINT pk_ndarray_type_measurement_type PRIMARY KEY (ndarray_type_id, measurement_metatype_id, measurement_type_id);


--
-- TOC entry 3752 (class 2606 OID 1955330)
-- Name: pk_observation; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY observation
    ADD CONSTRAINT pk_observation PRIMARY KEY (observation_type_id, observation_id);


--
-- TOC entry 3754 (class 2606 OID 1955332)
-- Name: pk_observation_type; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY observation_type
    ADD CONSTRAINT pk_observation_type PRIMARY KEY (observation_type_id);


--
-- TOC entry 3759 (class 2606 OID 1955334)
-- Name: pk_platform; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY platform
    ADD CONSTRAINT pk_platform PRIMARY KEY (platform_type_id, platform_id);


--
-- TOC entry 3763 (class 2606 OID 1955336)
-- Name: pk_platform_type; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY platform_type
    ADD CONSTRAINT pk_platform_type PRIMARY KEY (platform_type_id);


--
-- TOC entry 3687 (class 2606 OID 1955338)
-- Name: pk_reference_system; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY reference_system
    ADD CONSTRAINT pk_reference_system PRIMARY KEY (reference_system_id);


--
-- TOC entry 3693 (class 2606 OID 1955340)
-- Name: pk_reference_system_indexing; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY reference_system_indexing
    ADD CONSTRAINT pk_reference_system_indexing PRIMARY KEY (reference_system_id, array_index);


--
-- TOC entry 3699 (class 2606 OID 1955342)
-- Name: property_pkey; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY property
    ADD CONSTRAINT property_pkey PRIMARY KEY (property_id);


--
-- TOC entry 3636 (class 2606 OID 1955344)
-- Name: uq_dataset_type_dataset_type_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dataset_type
    ADD CONSTRAINT uq_dataset_type_dataset_type_name UNIQUE (dataset_type_name);


--
-- TOC entry 3652 (class 2606 OID 1955346)
-- Name: uq_dataset_type_measurement_type_ndarray_type_id_measurement_ty; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dataset_type_measurement_type
    ADD CONSTRAINT uq_dataset_type_measurement_type_ndarray_type_id_measurement_ty UNIQUE (dataset_type_id, measurement_type_index);


--
-- TOC entry 3656 (class 2606 OID 1955348)
-- Name: uq_datatype_datatype_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY datatype
    ADD CONSTRAINT uq_datatype_datatype_name UNIQUE (datatype_name);


--
-- TOC entry 3660 (class 2606 OID 1955350)
-- Name: uq_dimension_dimension_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dimension
    ADD CONSTRAINT uq_dimension_dimension_name UNIQUE (dimension_name);


--
-- TOC entry 3662 (class 2606 OID 1955352)
-- Name: uq_dimension_dimension_tag; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dimension
    ADD CONSTRAINT uq_dimension_dimension_tag UNIQUE (dimension_tag);


--
-- TOC entry 3670 (class 2606 OID 1955354)
-- Name: uq_domain_domain_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY domain
    ADD CONSTRAINT uq_domain_domain_name UNIQUE (domain_name);


--
-- TOC entry 3674 (class 2606 OID 1955356)
-- Name: uq_indexing_type_indexing_type_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY indexing_type
    ADD CONSTRAINT uq_indexing_type_indexing_type_name UNIQUE (indexing_type_name);


--
-- TOC entry 3707 (class 2606 OID 1955358)
-- Name: uq_instrument_instrument_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY instrument
    ADD CONSTRAINT uq_instrument_instrument_name UNIQUE (instrument_name);


--
-- TOC entry 3711 (class 2606 OID 1955360)
-- Name: uq_instrument_type_instrument_type_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY instrument_type
    ADD CONSTRAINT uq_instrument_type_instrument_type_name UNIQUE (instrument_type_name);


--
-- TOC entry 3715 (class 2606 OID 1955362)
-- Name: uq_measurement_metatype_measurement_metatype_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY measurement_metatype
    ADD CONSTRAINT uq_measurement_metatype_measurement_metatype_name UNIQUE (measurement_metatype_name);


--
-- TOC entry 3724 (class 2606 OID 1955364)
-- Name: uq_ndarray_ndarray_location; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY ndarray
    ADD CONSTRAINT uq_ndarray_ndarray_location UNIQUE (ndarray_location);


--
-- TOC entry 3685 (class 2606 OID 1955366)
-- Name: uq_ndarray_type_dimension_ndarray_type_dimension; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY ndarray_type_dimension
    ADD CONSTRAINT uq_ndarray_type_dimension_ndarray_type_dimension UNIQUE (ndarray_type_id, dimension_id);


--
-- TOC entry 4000 (class 0 OID 0)
-- Dependencies: 3685
-- Name: CONSTRAINT uq_ndarray_type_dimension_ndarray_type_dimension ON ndarray_type_dimension; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON CONSTRAINT uq_ndarray_type_dimension_ndarray_type_dimension ON ndarray_type_dimension IS 'Unique constraint to ensure each dimension is only represented once in each ndarray_type';


--
-- TOC entry 3748 (class 2606 OID 1955368)
-- Name: uq_ndarray_type_measurement_type_ndarray_type_id_measurement_ty; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY ndarray_type_measurement_type
    ADD CONSTRAINT uq_ndarray_type_measurement_type_ndarray_type_id_measurement_ty UNIQUE (ndarray_type_id, measurement_type_index);


--
-- TOC entry 3678 (class 2606 OID 1955370)
-- Name: uq_ndarray_type_ndarray_type_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY ndarray_type
    ADD CONSTRAINT uq_ndarray_type_ndarray_type_name UNIQUE (ndarray_type_name);


--
-- TOC entry 3756 (class 2606 OID 1955372)
-- Name: uq_observation_type_observation_type_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY observation_type
    ADD CONSTRAINT uq_observation_type_observation_type_name UNIQUE (observation_type_name);


--
-- TOC entry 3761 (class 2606 OID 1955374)
-- Name: uq_platform_platform_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY platform
    ADD CONSTRAINT uq_platform_platform_name UNIQUE (platform_name);


--
-- TOC entry 3765 (class 2606 OID 1955376)
-- Name: uq_platform_type_platform_type_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY platform_type
    ADD CONSTRAINT uq_platform_type_platform_type_name UNIQUE (platform_type_name);


--
-- TOC entry 3701 (class 2606 OID 1955378)
-- Name: uq_property_property_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY property
    ADD CONSTRAINT uq_property_property_name UNIQUE (property_name);


--
-- TOC entry 3689 (class 2606 OID 1955380)
-- Name: uq_reference_system_reference_system_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY reference_system
    ADD CONSTRAINT uq_reference_system_reference_system_name UNIQUE (reference_system_name);


SET search_path = earth_observation, pg_catalog;

--
-- TOC entry 3619 (class 1259 OID 1955381)
-- Name: fki_spectral_parameters_measurement_type; Type: INDEX; Schema: earth_observation; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_spectral_parameters_measurement_type ON spectral_parameters USING btree (measurement_metatype_id, measurement_type_id);


SET search_path = public, pg_catalog;

--
-- TOC entry 3622 (class 1259 OID 1955382)
-- Name: fki_dataset_dataset_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_dataset_type ON dataset USING btree (dataset_type_id);


--
-- TOC entry 3626 (class 1259 OID 1955383)
-- Name: fki_dataset_dimension_dataset; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_dimension_dataset ON dataset_dimension USING btree (dataset_type_id, dataset_id);


--
-- TOC entry 3627 (class 1259 OID 1955384)
-- Name: fki_dataset_dimension_dataset_type_dimension; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_dimension_dataset_type_dimension ON dataset_dimension USING btree (dataset_type_id, domain_id, dimension_id);


--
-- TOC entry 3630 (class 1259 OID 1955385)
-- Name: fki_dataset_metadata_dataset; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_metadata_dataset ON dataset_metadata USING btree (dataset_type_id, dataset_id);


--
-- TOC entry 3623 (class 1259 OID 1955386)
-- Name: fki_dataset_observation; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_observation ON dataset USING btree (observation_type_id, observation_id);


--
-- TOC entry 3637 (class 1259 OID 1955387)
-- Name: fki_dataset_type_dimension_dataset_type_domain; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_type_dimension_dataset_type_domain ON dataset_type_dimension USING btree (dataset_type_id, domain_id);


--
-- TOC entry 3638 (class 1259 OID 1955388)
-- Name: fki_dataset_type_dimension_dimension_domain; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_type_dimension_dimension_domain ON dataset_type_dimension USING btree (domain_id, dimension_id);


--
-- TOC entry 3641 (class 1259 OID 1955389)
-- Name: fki_dataset_type_domain_dataset_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_type_domain_dataset_type ON dataset_type_domain USING btree (dataset_type_id);


--
-- TOC entry 3642 (class 1259 OID 1955390)
-- Name: fki_dataset_type_domain_domain; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_type_domain_domain ON dataset_type_domain USING btree (domain_id);


--
-- TOC entry 3643 (class 1259 OID 1955391)
-- Name: fki_dataset_type_domain_reference_system; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_type_domain_reference_system ON dataset_type_domain USING btree (reference_system_id);


--
-- TOC entry 3646 (class 1259 OID 1955392)
-- Name: fki_dataset_type_measurement_metatype_datatype; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_type_measurement_metatype_datatype ON dataset_type_measurement_type USING btree (datatype_id);


--
-- TOC entry 3647 (class 1259 OID 1955393)
-- Name: fki_dataset_type_measurement_type_dataset_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_type_measurement_type_dataset_type ON dataset_type_measurement_type USING btree (dataset_type_id);


--
-- TOC entry 3648 (class 1259 OID 1955394)
-- Name: fki_dataset_type_measurement_type_measurement_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_type_measurement_type_measurement_type ON dataset_type_measurement_type USING btree (measurement_metatype_id, measurement_type_id);


--
-- TOC entry 3663 (class 1259 OID 1955395)
-- Name: fki_dimension_domain_dimension; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dimension_domain_dimension ON dimension_domain USING btree (dimension_id);


--
-- TOC entry 3664 (class 1259 OID 1955396)
-- Name: fki_dimension_domain_domain; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dimension_domain_domain ON dimension_domain USING btree (domain_id);


--
-- TOC entry 3702 (class 1259 OID 1955397)
-- Name: fki_instrument_instrument_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_instrument_instrument_type ON instrument USING btree (instrument_type_id);


--
-- TOC entry 3703 (class 1259 OID 1955398)
-- Name: fki_instrument_platform; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_instrument_platform ON instrument USING btree (platform_type_id, platform_id);


--
-- TOC entry 3716 (class 1259 OID 1955399)
-- Name: fki_measurement_type_measurement_metatype; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_measurement_type_measurement_metatype ON measurement_type USING btree (measurement_metatype_id);


--
-- TOC entry 3725 (class 1259 OID 1955400)
-- Name: fki_ndarray_dataset_dataset; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_ndarray_dataset_dataset ON ndarray_dataset USING btree (dataset_type_id, dataset_id);


--
-- TOC entry 3726 (class 1259 OID 1955401)
-- Name: fki_ndarray_dataset_ndarray; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_ndarray_dataset_ndarray ON ndarray_dataset USING btree (ndarray_type_id, ndarray_id, ndarray_version);


--
-- TOC entry 3729 (class 1259 OID 1955402)
-- Name: fki_ndarray_dimension_ndarray; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_ndarray_dimension_ndarray ON ndarray_dimension USING btree (ndarray_type_id, ndarray_id, ndarray_version);


--
-- TOC entry 3730 (class 1259 OID 1955403)
-- Name: fki_ndarray_dimension_ndarray_type_dimension; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_ndarray_dimension_ndarray_type_dimension ON ndarray_dimension USING btree (ndarray_type_id, domain_id, dimension_id);


--
-- TOC entry 3738 (class 1259 OID 1955404)
-- Name: fki_ndarray_footprint_dimension_ndarray; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_ndarray_footprint_dimension_ndarray ON ndarray_footprint_dimension USING btree (ndarray_type_id, ndarray_footprint_id);


--
-- TOC entry 3739 (class 1259 OID 1955405)
-- Name: fki_ndarray_footprint_dimension_ndarray_type_dimension; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_ndarray_footprint_dimension_ndarray_type_dimension ON ndarray_footprint_dimension USING btree (ndarray_type_id, domain_id, dimension_id);


--
-- TOC entry 3719 (class 1259 OID 1955406)
-- Name: fki_ndarray_footprint_ndarray_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_ndarray_footprint_ndarray_type ON ndarray USING btree (ndarray_type_id);


--
-- TOC entry 3720 (class 1259 OID 1955407)
-- Name: fki_ndarray_ndarray_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_ndarray_ndarray_type ON ndarray USING btree (ndarray_type_id, ndarray_type_id);


--
-- TOC entry 3694 (class 1259 OID 1955408)
-- Name: fki_ndarray_type_dimension_attribute_ndarray_type_dimension; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_ndarray_type_dimension_attribute_ndarray_type_dimension ON ndarray_type_dimension_property USING btree (ndarray_type_id, domain_id, dimension_id);


--
-- TOC entry 3695 (class 1259 OID 1955409)
-- Name: fki_ndarray_type_dimension_attribute_property; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_ndarray_type_dimension_attribute_property ON ndarray_type_dimension_property USING btree (property_id);


--
-- TOC entry 3679 (class 1259 OID 1955410)
-- Name: fki_ndarray_type_dimension_dimension_domain; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_ndarray_type_dimension_dimension_domain ON ndarray_type_dimension USING btree (domain_id, dimension_id);


--
-- TOC entry 3680 (class 1259 OID 1955411)
-- Name: fki_ndarray_type_dimension_indexing_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_ndarray_type_dimension_indexing_type ON ndarray_type_dimension USING btree (indexing_type_id);


--
-- TOC entry 3681 (class 1259 OID 1955412)
-- Name: fki_ndarray_type_dimension_ndarray_type_id_fkey; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_ndarray_type_dimension_ndarray_type_id_fkey ON ndarray_type_dimension USING btree (ndarray_type_id, domain_id);


--
-- TOC entry 3742 (class 1259 OID 1955413)
-- Name: fki_ndarray_type_masurement_type_datatype; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_ndarray_type_masurement_type_datatype ON ndarray_type_measurement_type USING btree (datatype_id);


--
-- TOC entry 3743 (class 1259 OID 1955414)
-- Name: fki_ndarray_type_measurement_type_measurement_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_ndarray_type_measurement_type_measurement_type ON ndarray_type_measurement_type USING btree (measurement_metatype_id, measurement_type_id);


--
-- TOC entry 3744 (class 1259 OID 1955415)
-- Name: fki_ndarray_type_measurement_type_ndarray_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_ndarray_type_measurement_type_ndarray_type ON ndarray_type_measurement_type USING btree (ndarray_type_id);


--
-- TOC entry 3749 (class 1259 OID 1955416)
-- Name: fki_observation_instrument; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_observation_instrument ON observation USING btree (instrument_type_id, instrument_id);


--
-- TOC entry 3750 (class 1259 OID 1955417)
-- Name: fki_observation_observation_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_observation_observation_type ON observation USING btree (observation_type_id);


--
-- TOC entry 3757 (class 1259 OID 1955418)
-- Name: fki_platform_platform_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_platform_platform_type ON platform USING btree (platform_type_id);


--
-- TOC entry 3690 (class 1259 OID 1955419)
-- Name: fki_reference_system_indexing_measurement_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_reference_system_indexing_measurement_type ON reference_system_indexing USING btree (measurement_metatype_id, measurement_type_id);


--
-- TOC entry 3691 (class 1259 OID 1955420)
-- Name: fki_reference_system_indexing_reference_system; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_reference_system_indexing_reference_system ON reference_system_indexing USING btree (reference_system_id);


--
-- TOC entry 3731 (class 1259 OID 1955421)
-- Name: idx_ndarray_dimension_ndarray_dimension_index; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX idx_ndarray_dimension_ndarray_dimension_index ON ndarray_dimension USING btree (ndarray_dimension_index);


--
-- TOC entry 3732 (class 1259 OID 1955422)
-- Name: idx_ndarray_dimension_ndarray_dimension_max; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX idx_ndarray_dimension_ndarray_dimension_max ON ndarray_dimension USING btree (ndarray_dimension_max);


--
-- TOC entry 3733 (class 1259 OID 1955423)
-- Name: idx_ndarray_dimension_ndarray_dimension_min; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX idx_ndarray_dimension_ndarray_dimension_min ON ndarray_dimension USING btree (ndarray_dimension_max);


SET search_path = earth_observation, pg_catalog;

--
-- TOC entry 3766 (class 2606 OID 1955424)
-- Name: fk_spectral_parameters_measurement_metatype; Type: FK CONSTRAINT; Schema: earth_observation; Owner: cube_admin
--

ALTER TABLE ONLY spectral_parameters
    ADD CONSTRAINT fk_spectral_parameters_measurement_metatype FOREIGN KEY (measurement_metatype_id, measurement_type_id) REFERENCES public.measurement_type(measurement_metatype_id, measurement_type_id);


SET search_path = public, pg_catalog;

--
-- TOC entry 3767 (class 2606 OID 1955429)
-- Name: fk_dataset_dataset_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset
    ADD CONSTRAINT fk_dataset_dataset_type FOREIGN KEY (dataset_type_id) REFERENCES dataset_type(dataset_type_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3769 (class 2606 OID 1955434)
-- Name: fk_dataset_dimension_dataset; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset_dimension
    ADD CONSTRAINT fk_dataset_dimension_dataset FOREIGN KEY (dataset_type_id, dataset_id) REFERENCES dataset(dataset_type_id, dataset_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3770 (class 2606 OID 1955439)
-- Name: fk_dataset_dimension_dataset_type_dimension; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset_dimension
    ADD CONSTRAINT fk_dataset_dimension_dataset_type_dimension FOREIGN KEY (dataset_type_id, domain_id, dimension_id) REFERENCES dataset_type_dimension(dataset_type_id, domain_id, dimension_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3771 (class 2606 OID 1955444)
-- Name: fk_dataset_metadata_dataset; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset_metadata
    ADD CONSTRAINT fk_dataset_metadata_dataset FOREIGN KEY (dataset_type_id, dataset_id) REFERENCES dataset(dataset_type_id, dataset_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3768 (class 2606 OID 1955449)
-- Name: fk_dataset_observation; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset
    ADD CONSTRAINT fk_dataset_observation FOREIGN KEY (observation_type_id, observation_id) REFERENCES observation(observation_type_id, observation_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3772 (class 2606 OID 1955454)
-- Name: fk_dataset_type_dimension_dataset_type_domain; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset_type_dimension
    ADD CONSTRAINT fk_dataset_type_dimension_dataset_type_domain FOREIGN KEY (dataset_type_id, domain_id) REFERENCES dataset_type_domain(dataset_type_id, domain_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3773 (class 2606 OID 1955459)
-- Name: fk_dataset_type_dimension_dimension_domain; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset_type_dimension
    ADD CONSTRAINT fk_dataset_type_dimension_dimension_domain FOREIGN KEY (domain_id, dimension_id) REFERENCES dimension_domain(domain_id, dimension_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3774 (class 2606 OID 1955464)
-- Name: fk_dataset_type_domain_dataset_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset_type_domain
    ADD CONSTRAINT fk_dataset_type_domain_dataset_type FOREIGN KEY (dataset_type_id) REFERENCES dataset_type(dataset_type_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3775 (class 2606 OID 1955469)
-- Name: fk_dataset_type_domain_domain; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset_type_domain
    ADD CONSTRAINT fk_dataset_type_domain_domain FOREIGN KEY (domain_id) REFERENCES domain(domain_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3776 (class 2606 OID 1955474)
-- Name: fk_dataset_type_domain_reference_system; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset_type_domain
    ADD CONSTRAINT fk_dataset_type_domain_reference_system FOREIGN KEY (reference_system_id) REFERENCES reference_system(reference_system_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3777 (class 2606 OID 1955479)
-- Name: fk_dataset_type_measurement_type_dataset_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset_type_measurement_type
    ADD CONSTRAINT fk_dataset_type_measurement_type_dataset_type FOREIGN KEY (dataset_type_id) REFERENCES dataset_type(dataset_type_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3778 (class 2606 OID 1955484)
-- Name: fk_dataset_type_measurement_type_datatype; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset_type_measurement_type
    ADD CONSTRAINT fk_dataset_type_measurement_type_datatype FOREIGN KEY (datatype_id) REFERENCES datatype(datatype_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3779 (class 2606 OID 1955489)
-- Name: fk_dataset_type_measurement_type_measurement_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset_type_measurement_type
    ADD CONSTRAINT fk_dataset_type_measurement_type_measurement_type FOREIGN KEY (measurement_metatype_id, measurement_type_id) REFERENCES measurement_type(measurement_metatype_id, measurement_type_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3780 (class 2606 OID 1955494)
-- Name: fk_dimension_domain_dimension; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dimension_domain
    ADD CONSTRAINT fk_dimension_domain_dimension FOREIGN KEY (dimension_id) REFERENCES dimension(dimension_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3781 (class 2606 OID 1955499)
-- Name: fk_dimension_domain_domain; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dimension_domain
    ADD CONSTRAINT fk_dimension_domain_domain FOREIGN KEY (domain_id) REFERENCES domain(domain_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3790 (class 2606 OID 1955504)
-- Name: fk_instrument_instrument_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY instrument
    ADD CONSTRAINT fk_instrument_instrument_type FOREIGN KEY (instrument_type_id) REFERENCES instrument_type(instrument_type_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3791 (class 2606 OID 1955509)
-- Name: fk_instrument_platform; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY instrument
    ADD CONSTRAINT fk_instrument_platform FOREIGN KEY (platform_type_id, platform_id) REFERENCES platform(platform_type_id, platform_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3792 (class 2606 OID 1955514)
-- Name: fk_measurement_type_measurement_metatype; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY measurement_type
    ADD CONSTRAINT fk_measurement_type_measurement_metatype FOREIGN KEY (measurement_metatype_id) REFERENCES measurement_metatype(measurement_metatype_id);


--
-- TOC entry 3794 (class 2606 OID 1955519)
-- Name: fk_ndarray_dataset_dataset; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY ndarray_dataset
    ADD CONSTRAINT fk_ndarray_dataset_dataset FOREIGN KEY (dataset_type_id, dataset_id) REFERENCES dataset(dataset_type_id, dataset_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3795 (class 2606 OID 1955524)
-- Name: fk_ndarray_dataset_ndarray; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY ndarray_dataset
    ADD CONSTRAINT fk_ndarray_dataset_ndarray FOREIGN KEY (ndarray_type_id, ndarray_id, ndarray_version) REFERENCES ndarray(ndarray_type_id, ndarray_id, ndarray_version) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3796 (class 2606 OID 1955529)
-- Name: fk_ndarray_dimension_ndarray; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY ndarray_dimension
    ADD CONSTRAINT fk_ndarray_dimension_ndarray FOREIGN KEY (ndarray_type_id, ndarray_id, ndarray_version) REFERENCES ndarray(ndarray_type_id, ndarray_id, ndarray_version) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3797 (class 2606 OID 1955534)
-- Name: fk_ndarray_dimension_ndarray_type_dimension; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY ndarray_dimension
    ADD CONSTRAINT fk_ndarray_dimension_ndarray_type_dimension FOREIGN KEY (ndarray_type_id, domain_id, dimension_id) REFERENCES ndarray_type_dimension(ndarray_type_id, domain_id, dimension_id);


--
-- TOC entry 3799 (class 2606 OID 1955539)
-- Name: fk_ndarray_footprint_dimension_ndarray_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY ndarray_footprint_dimension
    ADD CONSTRAINT fk_ndarray_footprint_dimension_ndarray_type FOREIGN KEY (ndarray_type_id, ndarray_footprint_id) REFERENCES ndarray_footprint(ndarray_type_id, ndarray_footprint_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3800 (class 2606 OID 1955544)
-- Name: fk_ndarray_footprint_dimension_ndarray_type_dimension; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY ndarray_footprint_dimension
    ADD CONSTRAINT fk_ndarray_footprint_dimension_ndarray_type_dimension FOREIGN KEY (ndarray_type_id, domain_id, dimension_id) REFERENCES ndarray_type_dimension(ndarray_type_id, domain_id, dimension_id);


--
-- TOC entry 3798 (class 2606 OID 1955549)
-- Name: fk_ndarray_footprint_ndarray_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY ndarray_footprint
    ADD CONSTRAINT fk_ndarray_footprint_ndarray_type FOREIGN KEY (ndarray_type_id) REFERENCES ndarray_type(ndarray_type_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3793 (class 2606 OID 1955554)
-- Name: fk_ndarray_ndarray_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY ndarray
    ADD CONSTRAINT fk_ndarray_ndarray_type FOREIGN KEY (ndarray_type_id) REFERENCES ndarray_type(ndarray_type_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3788 (class 2606 OID 1955559)
-- Name: fk_ndarray_type_dimension_attribute_ndarray_type_dimension; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY ndarray_type_dimension_property
    ADD CONSTRAINT fk_ndarray_type_dimension_attribute_ndarray_type_dimension FOREIGN KEY (ndarray_type_id, domain_id, dimension_id) REFERENCES ndarray_type_dimension(ndarray_type_id, domain_id, dimension_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3782 (class 2606 OID 1955564)
-- Name: fk_ndarray_type_dimension_dimension_domain; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY ndarray_type_dimension
    ADD CONSTRAINT fk_ndarray_type_dimension_dimension_domain FOREIGN KEY (domain_id, dimension_id) REFERENCES dimension_domain(domain_id, dimension_id) ON UPDATE CASCADE;


--
-- TOC entry 3783 (class 2606 OID 1955569)
-- Name: fk_ndarray_type_dimension_indexing_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY ndarray_type_dimension
    ADD CONSTRAINT fk_ndarray_type_dimension_indexing_type FOREIGN KEY (indexing_type_id) REFERENCES indexing_type(indexing_type_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3784 (class 2606 OID 1955574)
-- Name: fk_ndarray_type_dimension_ndarray_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY ndarray_type_dimension
    ADD CONSTRAINT fk_ndarray_type_dimension_ndarray_type FOREIGN KEY (ndarray_type_id) REFERENCES ndarray_type(ndarray_type_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3789 (class 2606 OID 1955579)
-- Name: fk_ndarray_type_dimension_property_property; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY ndarray_type_dimension_property
    ADD CONSTRAINT fk_ndarray_type_dimension_property_property FOREIGN KEY (property_id) REFERENCES property(property_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3785 (class 2606 OID 1955584)
-- Name: fk_ndarray_type_dimension_reference_system; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY ndarray_type_dimension
    ADD CONSTRAINT fk_ndarray_type_dimension_reference_system FOREIGN KEY (reference_system_id) REFERENCES reference_system(reference_system_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3801 (class 2606 OID 1955589)
-- Name: fk_ndarray_type_measurement_type_datatype; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY ndarray_type_measurement_type
    ADD CONSTRAINT fk_ndarray_type_measurement_type_datatype FOREIGN KEY (datatype_id) REFERENCES datatype(datatype_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3802 (class 2606 OID 1955594)
-- Name: fk_ndarray_type_measurement_type_measurement_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY ndarray_type_measurement_type
    ADD CONSTRAINT fk_ndarray_type_measurement_type_measurement_type FOREIGN KEY (measurement_metatype_id, measurement_type_id) REFERENCES measurement_type(measurement_metatype_id, measurement_type_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3803 (class 2606 OID 1955599)
-- Name: fk_ndarray_type_measurement_type_ndarray_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY ndarray_type_measurement_type
    ADD CONSTRAINT fk_ndarray_type_measurement_type_ndarray_type FOREIGN KEY (ndarray_type_id) REFERENCES ndarray_type(ndarray_type_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3804 (class 2606 OID 1955604)
-- Name: fk_observation_instrument; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY observation
    ADD CONSTRAINT fk_observation_instrument FOREIGN KEY (instrument_type_id, instrument_id) REFERENCES instrument(instrument_type_id, instrument_id);


--
-- TOC entry 3805 (class 2606 OID 1955609)
-- Name: fk_observation_observation_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY observation
    ADD CONSTRAINT fk_observation_observation_type FOREIGN KEY (observation_type_id) REFERENCES observation_type(observation_type_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3806 (class 2606 OID 1955614)
-- Name: fk_platform_platform_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY platform
    ADD CONSTRAINT fk_platform_platform_type FOREIGN KEY (platform_type_id) REFERENCES platform_type(platform_type_id);


--
-- TOC entry 3786 (class 2606 OID 1955619)
-- Name: fk_reference_system_indexing_measurement_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY reference_system_indexing
    ADD CONSTRAINT fk_reference_system_indexing_measurement_type FOREIGN KEY (measurement_metatype_id, measurement_type_id) REFERENCES measurement_type(measurement_metatype_id, measurement_type_id);


--
-- TOC entry 3787 (class 2606 OID 1955624)
-- Name: fk_reference_system_indexing_reference_system; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY reference_system_indexing
    ADD CONSTRAINT fk_reference_system_indexing_reference_system FOREIGN KEY (reference_system_id) REFERENCES reference_system(reference_system_id);


--
-- TOC entry 3930 (class 0 OID 0)
-- Dependencies: 9
-- Name: public; Type: ACL; Schema: -; Owner: postgres
--

REVOKE ALL ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM postgres;
GRANT ALL ON SCHEMA public TO postgres;
GRANT ALL ON SCHEMA public TO cube_admin;
GRANT ALL ON SCHEMA public TO PUBLIC;


--
-- TOC entry 3932 (class 0 OID 0)
-- Dependencies: 10
-- Name: ztmp; Type: ACL; Schema: -; Owner: postgres
--

REVOKE ALL ON SCHEMA ztmp FROM PUBLIC;
REVOKE ALL ON SCHEMA ztmp FROM postgres;
GRANT ALL ON SCHEMA ztmp TO postgres;
GRANT ALL ON SCHEMA ztmp TO PUBLIC;


SET search_path = earth_observation, pg_catalog;

--
-- TOC entry 3934 (class 0 OID 0)
-- Dependencies: 205
-- Name: spectral_parameters; Type: ACL; Schema: earth_observation; Owner: cube_admin
--

REVOKE ALL ON TABLE spectral_parameters FROM PUBLIC;
REVOKE ALL ON TABLE spectral_parameters FROM cube_admin;
GRANT ALL ON TABLE spectral_parameters TO cube_admin;
GRANT ALL ON TABLE spectral_parameters TO cube_admin_group;
GRANT SELECT ON TABLE spectral_parameters TO cube_user_group;


SET search_path = public, pg_catalog;

--
-- TOC entry 3936 (class 0 OID 0)
-- Dependencies: 206
-- Name: dataset; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE dataset FROM PUBLIC;
REVOKE ALL ON TABLE dataset FROM cube_admin;
GRANT ALL ON TABLE dataset TO cube_admin;
GRANT ALL ON TABLE dataset TO cube_admin_group;
GRANT SELECT ON TABLE dataset TO cube_user_group;


--
-- TOC entry 3938 (class 0 OID 0)
-- Dependencies: 207
-- Name: dataset_dimension; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE dataset_dimension FROM PUBLIC;
REVOKE ALL ON TABLE dataset_dimension FROM cube_admin;
GRANT ALL ON TABLE dataset_dimension TO cube_admin;
GRANT ALL ON TABLE dataset_dimension TO cube_admin_group;
GRANT SELECT ON TABLE dataset_dimension TO cube_user_group;


--
-- TOC entry 3940 (class 0 OID 0)
-- Dependencies: 208
-- Name: dataset_metadata; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE dataset_metadata FROM PUBLIC;
REVOKE ALL ON TABLE dataset_metadata FROM cube_admin;
GRANT ALL ON TABLE dataset_metadata TO cube_admin;
GRANT ALL ON TABLE dataset_metadata TO cube_admin_group;
GRANT SELECT ON TABLE dataset_metadata TO cube_user_group;


--
-- TOC entry 3942 (class 0 OID 0)
-- Dependencies: 209
-- Name: dataset_type; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE dataset_type FROM PUBLIC;
REVOKE ALL ON TABLE dataset_type FROM cube_admin;
GRANT ALL ON TABLE dataset_type TO cube_admin;
GRANT ALL ON TABLE dataset_type TO cube_admin_group;
GRANT SELECT ON TABLE dataset_type TO cube_user_group;


--
-- TOC entry 3944 (class 0 OID 0)
-- Dependencies: 210
-- Name: dataset_type_dimension; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE dataset_type_dimension FROM PUBLIC;
REVOKE ALL ON TABLE dataset_type_dimension FROM cube_admin;
GRANT ALL ON TABLE dataset_type_dimension TO cube_admin;
GRANT ALL ON TABLE dataset_type_dimension TO cube_admin_group;
GRANT SELECT ON TABLE dataset_type_dimension TO cube_user_group;


--
-- TOC entry 3946 (class 0 OID 0)
-- Dependencies: 211
-- Name: dataset_type_domain; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE dataset_type_domain FROM PUBLIC;
REVOKE ALL ON TABLE dataset_type_domain FROM cube_admin;
GRANT ALL ON TABLE dataset_type_domain TO cube_admin;
GRANT ALL ON TABLE dataset_type_domain TO cube_admin_group;
GRANT SELECT ON TABLE dataset_type_domain TO cube_user_group;


--
-- TOC entry 3948 (class 0 OID 0)
-- Dependencies: 212
-- Name: dataset_type_measurement_type; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE dataset_type_measurement_type FROM PUBLIC;
REVOKE ALL ON TABLE dataset_type_measurement_type FROM cube_admin;
GRANT ALL ON TABLE dataset_type_measurement_type TO cube_admin;
GRANT ALL ON TABLE dataset_type_measurement_type TO cube_admin_group;
GRANT SELECT ON TABLE dataset_type_measurement_type TO cube_user_group;


--
-- TOC entry 3950 (class 0 OID 0)
-- Dependencies: 213
-- Name: datatype; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE datatype FROM PUBLIC;
REVOKE ALL ON TABLE datatype FROM cube_admin;
GRANT ALL ON TABLE datatype TO cube_admin;
GRANT ALL ON TABLE datatype TO cube_admin_group;
GRANT SELECT ON TABLE datatype TO cube_user_group;


--
-- TOC entry 3952 (class 0 OID 0)
-- Dependencies: 214
-- Name: dimension; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE dimension FROM PUBLIC;
REVOKE ALL ON TABLE dimension FROM cube_admin;
GRANT ALL ON TABLE dimension TO cube_admin;
GRANT ALL ON TABLE dimension TO cube_admin_group;
GRANT SELECT ON TABLE dimension TO cube_user_group;


--
-- TOC entry 3954 (class 0 OID 0)
-- Dependencies: 215
-- Name: dimension_domain; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE dimension_domain FROM PUBLIC;
REVOKE ALL ON TABLE dimension_domain FROM cube_admin;
GRANT ALL ON TABLE dimension_domain TO cube_admin;
GRANT ALL ON TABLE dimension_domain TO cube_admin_group;
GRANT SELECT ON TABLE dimension_domain TO cube_user_group;


--
-- TOC entry 3956 (class 0 OID 0)
-- Dependencies: 216
-- Name: domain; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE domain FROM PUBLIC;
REVOKE ALL ON TABLE domain FROM cube_admin;
GRANT ALL ON TABLE domain TO cube_admin;
GRANT ALL ON TABLE domain TO cube_admin_group;
GRANT SELECT ON TABLE domain TO cube_user_group;


--
-- TOC entry 3958 (class 0 OID 0)
-- Dependencies: 217
-- Name: indexing_type; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE indexing_type FROM PUBLIC;
REVOKE ALL ON TABLE indexing_type FROM cube_admin;
GRANT ALL ON TABLE indexing_type TO cube_admin;
GRANT ALL ON TABLE indexing_type TO cube_admin_group;
GRANT SELECT ON TABLE indexing_type TO cube_user_group;


--
-- TOC entry 3960 (class 0 OID 0)
-- Dependencies: 218
-- Name: ndarray_type; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE ndarray_type FROM PUBLIC;
REVOKE ALL ON TABLE ndarray_type FROM cube_admin;
GRANT ALL ON TABLE ndarray_type TO cube_admin;
GRANT ALL ON TABLE ndarray_type TO cube_admin_group;
GRANT SELECT ON TABLE ndarray_type TO cube_user_group;


--
-- TOC entry 3962 (class 0 OID 0)
-- Dependencies: 219
-- Name: ndarray_type_dimension; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE ndarray_type_dimension FROM PUBLIC;
REVOKE ALL ON TABLE ndarray_type_dimension FROM cube_admin;
GRANT ALL ON TABLE ndarray_type_dimension TO cube_admin;
GRANT ALL ON TABLE ndarray_type_dimension TO cube_admin_group;
GRANT SELECT ON TABLE ndarray_type_dimension TO cube_user_group;


--
-- TOC entry 3964 (class 0 OID 0)
-- Dependencies: 220
-- Name: reference_system; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE reference_system FROM PUBLIC;
REVOKE ALL ON TABLE reference_system FROM cube_admin;
GRANT ALL ON TABLE reference_system TO cube_admin;
GRANT ALL ON TABLE reference_system TO cube_admin_group;
GRANT SELECT ON TABLE reference_system TO cube_user_group;


--
-- TOC entry 3965 (class 0 OID 0)
-- Dependencies: 258
-- Name: ndarray_type_dimension_view; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE ndarray_type_dimension_view FROM PUBLIC;
REVOKE ALL ON TABLE ndarray_type_dimension_view FROM cube_admin;
GRANT ALL ON TABLE ndarray_type_dimension_view TO cube_admin;
GRANT ALL ON TABLE ndarray_type_dimension_view TO cube_admin_group;
GRANT SELECT ON TABLE ndarray_type_dimension_view TO cube_user_group;


--
-- TOC entry 3967 (class 0 OID 0)
-- Dependencies: 221
-- Name: reference_system_indexing; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE reference_system_indexing FROM PUBLIC;
REVOKE ALL ON TABLE reference_system_indexing FROM cube_admin;
GRANT ALL ON TABLE reference_system_indexing TO cube_admin;
GRANT ALL ON TABLE reference_system_indexing TO cube_admin_group;
GRANT SELECT ON TABLE reference_system_indexing TO cube_user_group;


--
-- TOC entry 3968 (class 0 OID 0)
-- Dependencies: 259
-- Name: dimension_indices_view; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE dimension_indices_view FROM PUBLIC;
REVOKE ALL ON TABLE dimension_indices_view FROM cube_admin;
GRANT ALL ON TABLE dimension_indices_view TO cube_admin;
GRANT ALL ON TABLE dimension_indices_view TO cube_admin_group;
GRANT SELECT ON TABLE dimension_indices_view TO cube_user_group;


--
-- TOC entry 3969 (class 0 OID 0)
-- Dependencies: 222
-- Name: ndarray_type_dimension_property; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE ndarray_type_dimension_property FROM PUBLIC;
REVOKE ALL ON TABLE ndarray_type_dimension_property FROM cube_admin;
GRANT ALL ON TABLE ndarray_type_dimension_property TO cube_admin;
GRANT ALL ON TABLE ndarray_type_dimension_property TO cube_admin_group;
GRANT SELECT ON TABLE ndarray_type_dimension_property TO cube_user_group;


--
-- TOC entry 3970 (class 0 OID 0)
-- Dependencies: 223
-- Name: property; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE property FROM PUBLIC;
REVOKE ALL ON TABLE property FROM cube_admin;
GRANT ALL ON TABLE property TO cube_admin;
GRANT ALL ON TABLE property TO cube_admin_group;
GRANT SELECT ON TABLE property TO cube_user_group;


--
-- TOC entry 3971 (class 0 OID 0)
-- Dependencies: 260
-- Name: dimension_properties_view; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE dimension_properties_view FROM PUBLIC;
REVOKE ALL ON TABLE dimension_properties_view FROM cube_admin;
GRANT ALL ON TABLE dimension_properties_view TO cube_admin;
GRANT ALL ON TABLE dimension_properties_view TO cube_admin_group;
GRANT SELECT ON TABLE dimension_properties_view TO cube_user_group;


--
-- TOC entry 3973 (class 0 OID 0)
-- Dependencies: 224
-- Name: instrument; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE instrument FROM PUBLIC;
REVOKE ALL ON TABLE instrument FROM cube_admin;
GRANT ALL ON TABLE instrument TO cube_admin;
GRANT ALL ON TABLE instrument TO cube_admin_group;
GRANT SELECT ON TABLE instrument TO cube_user_group;


--
-- TOC entry 3975 (class 0 OID 0)
-- Dependencies: 225
-- Name: instrument_type; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE instrument_type FROM PUBLIC;
REVOKE ALL ON TABLE instrument_type FROM cube_admin;
GRANT ALL ON TABLE instrument_type TO cube_admin;
GRANT ALL ON TABLE instrument_type TO cube_admin_group;
GRANT SELECT ON TABLE instrument_type TO cube_user_group;


--
-- TOC entry 3977 (class 0 OID 0)
-- Dependencies: 226
-- Name: measurement_metatype; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE measurement_metatype FROM PUBLIC;
REVOKE ALL ON TABLE measurement_metatype FROM cube_admin;
GRANT ALL ON TABLE measurement_metatype TO cube_admin;
GRANT ALL ON TABLE measurement_metatype TO cube_admin_group;
GRANT SELECT ON TABLE measurement_metatype TO cube_user_group;


--
-- TOC entry 3979 (class 0 OID 0)
-- Dependencies: 227
-- Name: measurement_type; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE measurement_type FROM PUBLIC;
REVOKE ALL ON TABLE measurement_type FROM cube_admin;
GRANT ALL ON TABLE measurement_type TO cube_admin;
GRANT ALL ON TABLE measurement_type TO cube_admin_group;
GRANT SELECT ON TABLE measurement_type TO cube_user_group;


--
-- TOC entry 3981 (class 0 OID 0)
-- Dependencies: 228
-- Name: ndarray; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE ndarray FROM PUBLIC;
REVOKE ALL ON TABLE ndarray FROM cube_admin;
GRANT ALL ON TABLE ndarray TO cube_admin;
GRANT ALL ON TABLE ndarray TO cube_admin_group;
GRANT SELECT ON TABLE ndarray TO cube_user_group;


--
-- TOC entry 3983 (class 0 OID 0)
-- Dependencies: 229
-- Name: ndarray_dataset; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE ndarray_dataset FROM PUBLIC;
REVOKE ALL ON TABLE ndarray_dataset FROM cube_admin;
GRANT ALL ON TABLE ndarray_dataset TO cube_admin;
GRANT ALL ON TABLE ndarray_dataset TO cube_admin_group;
GRANT SELECT ON TABLE ndarray_dataset TO cube_user_group;


--
-- TOC entry 3985 (class 0 OID 0)
-- Dependencies: 230
-- Name: ndarray_dimension; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE ndarray_dimension FROM PUBLIC;
REVOKE ALL ON TABLE ndarray_dimension FROM cube_admin;
GRANT ALL ON TABLE ndarray_dimension TO cube_admin;
GRANT ALL ON TABLE ndarray_dimension TO cube_admin_group;
GRANT SELECT ON TABLE ndarray_dimension TO cube_user_group;


--
-- TOC entry 3987 (class 0 OID 0)
-- Dependencies: 231
-- Name: ndarray_footprint; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE ndarray_footprint FROM PUBLIC;
REVOKE ALL ON TABLE ndarray_footprint FROM cube_admin;
GRANT ALL ON TABLE ndarray_footprint TO cube_admin;
GRANT ALL ON TABLE ndarray_footprint TO cube_admin_group;
GRANT SELECT ON TABLE ndarray_footprint TO cube_user_group;


--
-- TOC entry 3989 (class 0 OID 0)
-- Dependencies: 232
-- Name: ndarray_footprint_dimension; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE ndarray_footprint_dimension FROM PUBLIC;
REVOKE ALL ON TABLE ndarray_footprint_dimension FROM cube_admin;
GRANT ALL ON TABLE ndarray_footprint_dimension TO cube_admin;
GRANT ALL ON TABLE ndarray_footprint_dimension TO cube_admin_group;
GRANT SELECT ON TABLE ndarray_footprint_dimension TO cube_user_group;


--
-- TOC entry 3991 (class 0 OID 0)
-- Dependencies: 233
-- Name: ndarray_type_measurement_type; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE ndarray_type_measurement_type FROM PUBLIC;
REVOKE ALL ON TABLE ndarray_type_measurement_type FROM cube_admin;
GRANT ALL ON TABLE ndarray_type_measurement_type TO cube_admin;
GRANT ALL ON TABLE ndarray_type_measurement_type TO cube_admin_group;
GRANT SELECT ON TABLE ndarray_type_measurement_type TO cube_user_group;


--
-- TOC entry 3993 (class 0 OID 0)
-- Dependencies: 234
-- Name: observation; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE observation FROM PUBLIC;
REVOKE ALL ON TABLE observation FROM cube_admin;
GRANT ALL ON TABLE observation TO cube_admin;
GRANT ALL ON TABLE observation TO cube_admin_group;
GRANT SELECT ON TABLE observation TO cube_user_group;


--
-- TOC entry 3995 (class 0 OID 0)
-- Dependencies: 235
-- Name: observation_type; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE observation_type FROM PUBLIC;
REVOKE ALL ON TABLE observation_type FROM cube_admin;
GRANT ALL ON TABLE observation_type TO cube_admin;
GRANT ALL ON TABLE observation_type TO cube_admin_group;
GRANT SELECT ON TABLE observation_type TO cube_user_group;


--
-- TOC entry 3997 (class 0 OID 0)
-- Dependencies: 236
-- Name: platform; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE platform FROM PUBLIC;
REVOKE ALL ON TABLE platform FROM cube_admin;
GRANT ALL ON TABLE platform TO cube_admin;
GRANT ALL ON TABLE platform TO cube_admin_group;
GRANT SELECT ON TABLE platform TO cube_user_group;


--
-- TOC entry 3999 (class 0 OID 0)
-- Dependencies: 237
-- Name: platform_type; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE platform_type FROM PUBLIC;
REVOKE ALL ON TABLE platform_type FROM cube_admin;
GRANT ALL ON TABLE platform_type TO cube_admin;
GRANT ALL ON TABLE platform_type TO cube_admin_group;
GRANT SELECT ON TABLE platform_type TO cube_user_group;


-- Completed on 2015-04-05 17:09:42 AEST

--
-- PostgreSQL database dump complete
--

