--
-- PostgreSQL database dump
--

-- Dumped from database version 9.3.7
-- Dumped by pg_dump version 9.3.7
-- Started on 2015-05-28 16:28:25 AEST

SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;

--
-- TOC entry 8 (class 2615 OID 2485962)
-- Name: earth_observation; Type: SCHEMA; Schema: -; Owner: cube_admin
--

CREATE SCHEMA earth_observation;


ALTER SCHEMA earth_observation OWNER TO cube_admin;

--
-- TOC entry 6 (class 2615 OID 2482737)
-- Name: topology; Type: SCHEMA; Schema: -; Owner: cube_admin
--

CREATE SCHEMA topology;


ALTER SCHEMA topology OWNER TO cube_admin;

--
-- TOC entry 10 (class 2615 OID 2482738)
-- Name: ztmp; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA ztmp;


ALTER SCHEMA ztmp OWNER TO postgres;

--
-- TOC entry 4609 (class 0 OID 0)
-- Dependencies: 10
-- Name: SCHEMA ztmp; Type: COMMENT; Schema: -; Owner: postgres
--

COMMENT ON SCHEMA ztmp IS 'Temporary schema';


SET search_path = public, pg_catalog;

--
-- TOC entry 1931 (class 1247 OID 2485965)
-- Name: attribute_value_type; Type: TYPE; Schema: public; Owner: cube_admin
--

CREATE TYPE attribute_value_type AS (
	attribute_type_id bigint,
	attribute_id bigint,
	attribute_value_id bigint
);


ALTER TYPE public.attribute_value_type OWNER TO cube_admin;

--
-- TOC entry 1934 (class 1247 OID 2485968)
-- Name: category_id_level_type; Type: TYPE; Schema: public; Owner: cube_admin
--

CREATE TYPE category_id_level_type AS (
	category_type_id bigint,
	category_id bigint,
	level integer
);


ALTER TYPE public.category_id_level_type OWNER TO cube_admin;

--
-- TOC entry 1937 (class 1247 OID 2485971)
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
-- TOC entry 224 (class 1259 OID 2485972)
-- Name: spectral_parameters; Type: TABLE; Schema: earth_observation; Owner: cube_admin; Tablespace: 
--

CREATE TABLE spectral_parameters (
    measurement_metatype_id bigint NOT NULL,
    measurement_type_id bigint NOT NULL
);


ALTER TABLE earth_observation.spectral_parameters OWNER TO cube_admin;

--
-- TOC entry 4611 (class 0 OID 0)
-- Dependencies: 224
-- Name: TABLE spectral_parameters; Type: COMMENT; Schema: earth_observation; Owner: cube_admin
--

COMMENT ON TABLE spectral_parameters IS 'Configuration: Spectral band parameters';


SET search_path = public, pg_catalog;

--
-- TOC entry 225 (class 1259 OID 2485975)
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
-- TOC entry 4613 (class 0 OID 0)
-- Dependencies: 225
-- Name: TABLE dataset; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE dataset IS 'Data: Source dataset (file) ingested.
An example would be a dataset for a particular NBAR Landsat scene.';


--
-- TOC entry 226 (class 1259 OID 2485978)
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
-- TOC entry 4615 (class 0 OID 0)
-- Dependencies: 226
-- Name: TABLE dataset_dimension; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE dataset_dimension IS 'Data: Dimensional parameters for each source dataset.
Each dataset/dimension will have specific max/min/indexing values showing the range covered by the dataset in that particular dimension.';


--
-- TOC entry 227 (class 1259 OID 2485981)
-- Name: dataset_metadata; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE dataset_metadata (
    dataset_type_id bigint NOT NULL,
    dataset_id bigint NOT NULL,
    metadata_xml xml NOT NULL
);


ALTER TABLE public.dataset_metadata OWNER TO cube_admin;

--
-- TOC entry 4617 (class 0 OID 0)
-- Dependencies: 227
-- Name: TABLE dataset_metadata; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE dataset_metadata IS 'Data: Lookup table for dataset-level metadata (one:one)';


--
-- TOC entry 228 (class 1259 OID 2485987)
-- Name: dataset_type; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE dataset_type (
    dataset_type_id bigint NOT NULL,
    dataset_type_name character varying(254)
);


ALTER TABLE public.dataset_type OWNER TO cube_admin;

--
-- TOC entry 4619 (class 0 OID 0)
-- Dependencies: 228
-- Name: TABLE dataset_type; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE dataset_type IS 'Configuration: Type of source dataset (processing level)';


--
-- TOC entry 229 (class 1259 OID 2485990)
-- Name: dataset_type_dimension; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE dataset_type_dimension (
    dataset_type_id bigint NOT NULL,
    domain_id bigint NOT NULL,
    dimension_id bigint NOT NULL
);


ALTER TABLE public.dataset_type_dimension OWNER TO cube_admin;

--
-- TOC entry 4621 (class 0 OID 0)
-- Dependencies: 229
-- Name: TABLE dataset_type_dimension; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE dataset_type_dimension IS 'Configuration: Association between dataset type and dimensions. Used to define dimensionality of source dataset types';


--
-- TOC entry 230 (class 1259 OID 2485993)
-- Name: dataset_type_domain; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE dataset_type_domain (
    dataset_type_id bigint NOT NULL,
    domain_id bigint NOT NULL,
    reference_system_id bigint
);


ALTER TABLE public.dataset_type_domain OWNER TO cube_admin;

--
-- TOC entry 4623 (class 0 OID 0)
-- Dependencies: 230
-- Name: TABLE dataset_type_domain; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE dataset_type_domain IS 'Configuration: Association between dataset types and domains (many-many).
Used to define which domains cover a given dataset type';


--
-- TOC entry 231 (class 1259 OID 2485996)
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
-- TOC entry 4625 (class 0 OID 0)
-- Dependencies: 231
-- Name: TABLE dataset_type_measurement_type; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE dataset_type_measurement_type IS 'Configuration: Associations between dataset types and measurement types (one-many)
e.g. associations between Landsat 7 NBAR and specific surface-reflectance corrected Landsat 7 bands';


--
-- TOC entry 232 (class 1259 OID 2485999)
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
-- TOC entry 4627 (class 0 OID 0)
-- Dependencies: 232
-- Name: TABLE datatype; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE datatype IS 'Configuration: Lookup table for measurement_type datatypes.';


--
-- TOC entry 233 (class 1259 OID 2486002)
-- Name: dimension; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE dimension (
    dimension_id bigint NOT NULL,
    dimension_name character varying(50) NOT NULL,
    dimension_tag character varying(8) NOT NULL
);


ALTER TABLE public.dimension OWNER TO cube_admin;

--
-- TOC entry 4629 (class 0 OID 0)
-- Dependencies: 233
-- Name: TABLE dimension; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE dimension IS 'Configuration: Dimensions for n-dimensional data structures, e.g. x,y,z,t';


--
-- TOC entry 234 (class 1259 OID 2486005)
-- Name: dimension_domain; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE dimension_domain (
    domain_id bigint NOT NULL,
    dimension_id bigint NOT NULL
);


ALTER TABLE public.dimension_domain OWNER TO cube_admin;

--
-- TOC entry 4631 (class 0 OID 0)
-- Dependencies: 234
-- Name: TABLE dimension_domain; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE dimension_domain IS 'Configuration: Many-many  mapping between dimensions and domains to allow multiple dimensions to be included in multiple domains.
For example, the z dimension could be managed in a Z-spatial domain, or in an XYZ-spatial domain.';


--
-- TOC entry 235 (class 1259 OID 2486008)
-- Name: domain; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE domain (
    domain_id bigint NOT NULL,
    domain_name character varying(16),
    domain_tag character varying(16)
);


ALTER TABLE public.domain OWNER TO cube_admin;

--
-- TOC entry 4633 (class 0 OID 0)
-- Dependencies: 235
-- Name: TABLE domain; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE domain IS 'Configuration: Domain groupings of dimensions (e.g. spectral, spatial XY, spatial XYZ, temporal)';


--
-- TOC entry 236 (class 1259 OID 2486011)
-- Name: indexing_type; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE indexing_type (
    indexing_type_id smallint NOT NULL,
    indexing_type_name character varying(128)
);


ALTER TABLE public.indexing_type OWNER TO cube_admin;

--
-- TOC entry 4635 (class 0 OID 0)
-- Dependencies: 236
-- Name: TABLE indexing_type; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE indexing_type IS 'Configuration: Lookup table to manage what kind of indexing to apply to a given dimension';


--
-- TOC entry 239 (class 1259 OID 2486020)
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
-- TOC entry 4637 (class 0 OID 0)
-- Dependencies: 239
-- Name: TABLE reference_system; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE reference_system IS 'Configuration: Coordinate reference systems for aplication to specific domains.
e.g. EPSG:4326, seconds since 1/1/1970 0:00, etc.';


--
-- TOC entry 240 (class 1259 OID 2486031)
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
-- TOC entry 4639 (class 0 OID 0)
-- Dependencies: 240
-- Name: TABLE reference_system_indexing; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE reference_system_indexing IS 'Configuration: Optional non-linear indexing for dimension in a given domain.
e.g. A spectral dimension containing multple bands needs to be indexed by band number, and each band number can be associated with a given measurement_type.';


--
-- TOC entry 237 (class 1259 OID 2486014)
-- Name: storage_type; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE storage_type (
    storage_type_id bigint NOT NULL,
    storage_type_name character varying(254),
    storage_type_tag character varying(16)
);


ALTER TABLE public.storage_type OWNER TO cube_admin;

--
-- TOC entry 4641 (class 0 OID 0)
-- Dependencies: 237
-- Name: TABLE storage_type; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE storage_type IS 'Configuration: storage parameter lookup table. Used TO manage different storage_types';


--
-- TOC entry 238 (class 1259 OID 2486017)
-- Name: storage_type_dimension; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE storage_type_dimension (
    storage_type_id bigint NOT NULL,
    domain_id bigint NOT NULL,
    dimension_id bigint NOT NULL,
    dimension_order smallint,
    dimension_extent double precision,
    dimension_elements bigint,
    dimension_cache bigint,
    dimension_origin double precision,
    indexing_type_id smallint,
    reference_system_id bigint,
    index_reference_system_id bigint
);


ALTER TABLE public.storage_type_dimension OWNER TO cube_admin;

--
-- TOC entry 4643 (class 0 OID 0)
-- Dependencies: 238
-- Name: TABLE storage_type_dimension; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE storage_type_dimension IS 'Configuration: Association between storage type and dimensions. Used TO define dimensionality of storage type';


--
-- TOC entry 257 (class 1259 OID 2486543)
-- Name: storage_type_dimension_view; Type: MATERIALIZED VIEW; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE MATERIALIZED VIEW storage_type_dimension_view AS
 SELECT storage_type.storage_type_id,
    storage_type_dimension.dimension_order AS creation_order,
    domain.domain_id,
    dimension.dimension_id,
    storage_type_dimension.reference_system_id,
    storage_type.storage_type_name,
    dimension.dimension_name,
    dimension.dimension_tag,
    domain.domain_name,
    reference_system.reference_system_name,
    reference_system.reference_system_definition,
    indexing_type.indexing_type_name,
    storage_type_dimension.dimension_origin,
    storage_type_dimension.dimension_extent,
    index_reference_system.reference_system_unit AS index_unit,
    storage_type_dimension.dimension_elements,
    reference_system.reference_system_unit,
    storage_type_dimension.dimension_cache
   FROM (((((((storage_type storage_type
     JOIN storage_type_dimension storage_type_dimension USING (storage_type_id))
     JOIN dimension_domain USING (domain_id, dimension_id))
     JOIN domain USING (domain_id))
     JOIN dimension USING (dimension_id))
     JOIN reference_system USING (reference_system_id))
     JOIN reference_system index_reference_system ON ((storage_type_dimension.index_reference_system_id = index_reference_system.reference_system_id)))
     JOIN indexing_type USING (indexing_type_id))
  ORDER BY storage_type.storage_type_id, storage_type_dimension.dimension_order
  WITH NO DATA;


ALTER TABLE public.storage_type_dimension_view OWNER TO cube_admin;

--
-- TOC entry 258 (class 1259 OID 2486551)
-- Name: dimension_indices_view; Type: MATERIALIZED VIEW; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE MATERIALIZED VIEW dimension_indices_view AS
 SELECT storage_type_dimension_view.storage_type_id,
    storage_type_dimension_view.domain_id,
    storage_type_dimension_view.dimension_id,
    reference_system_indexing.reference_system_id,
    reference_system_indexing.array_index,
    reference_system_indexing.indexing_name,
    reference_system_indexing.measurement_metatype_id,
    reference_system_indexing.measurement_type_id
   FROM (storage_type_dimension_view
     JOIN reference_system_indexing USING (reference_system_id))
  ORDER BY storage_type_dimension_view.storage_type_id, storage_type_dimension_view.dimension_id, reference_system_indexing.array_index
  WITH NO DATA;


ALTER TABLE public.dimension_indices_view OWNER TO cube_admin;

--
-- TOC entry 242 (class 1259 OID 2486041)
-- Name: property; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE property (
    property_id bigint NOT NULL,
    property_name character varying(32),
    datatype_id smallint
);


ALTER TABLE public.property OWNER TO cube_admin;

--
-- TOC entry 4647 (class 0 OID 0)
-- Dependencies: 242
-- Name: TABLE property; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE property IS 'Configuration: Lookup table for dimension property';


--
-- TOC entry 241 (class 1259 OID 2486038)
-- Name: storage_type_dimension_property; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE storage_type_dimension_property (
    storage_type_id bigint NOT NULL,
    domain_id bigint NOT NULL,
    dimension_id bigint NOT NULL,
    property_id bigint NOT NULL,
    attribute_string character varying(128) NOT NULL
);


ALTER TABLE public.storage_type_dimension_property OWNER TO cube_admin;

--
-- TOC entry 4649 (class 0 OID 0)
-- Dependencies: 241
-- Name: TABLE storage_type_dimension_property; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE storage_type_dimension_property IS 'Configuration: Metadata properties of dimension in storage type';


--
-- TOC entry 259 (class 1259 OID 2486555)
-- Name: dimension_properties_view; Type: MATERIALIZED VIEW; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE MATERIALIZED VIEW dimension_properties_view AS
 SELECT storage_type_dimension_view.storage_type_id,
    storage_type_dimension_view.domain_id,
    storage_type_dimension_view.dimension_id,
    storage_type_dimension_view.dimension_name,
    property.property_name,
    storage_type_dimension_property.attribute_string,
    datatype.datatype_name
   FROM (((storage_type_dimension_view
     JOIN storage_type_dimension_property storage_type_dimension_property USING (storage_type_id, domain_id, dimension_id))
     JOIN property USING (property_id))
     JOIN datatype USING (datatype_id))
  ORDER BY storage_type_dimension_view.storage_type_id, storage_type_dimension_view.creation_order, property.property_name
  WITH NO DATA;


ALTER TABLE public.dimension_properties_view OWNER TO cube_admin;

--
-- TOC entry 243 (class 1259 OID 2486049)
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
-- TOC entry 4652 (class 0 OID 0)
-- Dependencies: 243
-- Name: TABLE instrument; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE instrument IS 'Configuration: Instrument used to gather observations.
An example would be the ETM+ sensor on the Landsat 7 platform';


--
-- TOC entry 244 (class 1259 OID 2486052)
-- Name: instrument_type; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE instrument_type (
    instrument_type_id bigint NOT NULL,
    instrument_type_name character varying(128)
);


ALTER TABLE public.instrument_type OWNER TO cube_admin;

--
-- TOC entry 4654 (class 0 OID 0)
-- Dependencies: 244
-- Name: TABLE instrument_type; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE instrument_type IS 'Configuration: Lookup table for instrument category';


--
-- TOC entry 245 (class 1259 OID 2486055)
-- Name: measurement_metatype; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE measurement_metatype (
    measurement_metatype_id bigint NOT NULL,
    measurement_metatype_name character varying(254)
);


ALTER TABLE public.measurement_metatype OWNER TO cube_admin;

--
-- TOC entry 4656 (class 0 OID 0)
-- Dependencies: 245
-- Name: TABLE measurement_metatype; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE measurement_metatype IS 'Configuration: General type of measurement (e.g. spectral band)';


--
-- TOC entry 246 (class 1259 OID 2486058)
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
-- TOC entry 4658 (class 0 OID 0)
-- Dependencies: 246
-- Name: TABLE measurement_type; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE measurement_type IS 'Configuration: Description of measurement(s) held in n-dimensional data structures: e.g. bands';


--
-- TOC entry 252 (class 1259 OID 2486084)
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
-- TOC entry 4660 (class 0 OID 0)
-- Dependencies: 252
-- Name: TABLE observation; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE observation IS 'Data: Source observation for datasets.
Analagous to old "acquisition" table in AGDC version 0 DB';


--
-- TOC entry 253 (class 1259 OID 2486087)
-- Name: observation_type; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE observation_type (
    observation_type_id bigint NOT NULL,
    observation_type_name character varying(254)
);


ALTER TABLE public.observation_type OWNER TO cube_admin;

--
-- TOC entry 4662 (class 0 OID 0)
-- Dependencies: 253
-- Name: TABLE observation_type; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE observation_type IS 'Configuration: Type of source observation';


--
-- TOC entry 254 (class 1259 OID 2486090)
-- Name: platform; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE platform (
    platform_type_id bigint NOT NULL,
    platform_id bigint NOT NULL,
    platform_name character varying(128)
);


ALTER TABLE public.platform OWNER TO cube_admin;

--
-- TOC entry 4664 (class 0 OID 0)
-- Dependencies: 254
-- Name: TABLE platform; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE platform IS 'Configuration: Platform on which instrument is mounted.
An example would be a specific satellite such as Landsat 7';


--
-- TOC entry 255 (class 1259 OID 2486093)
-- Name: platform_type; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE platform_type (
    platform_type_id bigint NOT NULL,
    platform_type_name character varying(128)
);


ALTER TABLE public.platform_type OWNER TO cube_admin;

--
-- TOC entry 4666 (class 0 OID 0)
-- Dependencies: 255
-- Name: TABLE platform_type; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE platform_type IS 'Configuration: Lookup table for platform category
e.g. Satellite or Ship';


--
-- TOC entry 256 (class 1259 OID 2486529)
-- Name: spatial_footprint; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE spatial_footprint (
    spatial_footprint_id bigint NOT NULL,
    spatial_footprint_geometry geometry NOT NULL
);


ALTER TABLE public.spatial_footprint OWNER TO cube_admin;

--
-- TOC entry 4668 (class 0 OID 0)
-- Dependencies: 256
-- Name: TABLE spatial_footprint; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE spatial_footprint IS 'Data: Spatial footprints';


--
-- TOC entry 247 (class 1259 OID 2486061)
-- Name: storage; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE storage (
    storage_type_id bigint NOT NULL,
    storage_id bigint NOT NULL,
    storage_version integer NOT NULL,
    storage_location character varying(354),
    md5_checksum character(32),
    storage_bytes bigint,
    spatial_footprint_id bigint
);


ALTER TABLE public.storage OWNER TO cube_admin;

--
-- TOC entry 4669 (class 0 OID 0)
-- Dependencies: 247
-- Name: TABLE storage; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE storage IS 'Data: n-dimensional data structure instances';


--
-- TOC entry 248 (class 1259 OID 2486064)
-- Name: storage_dataset; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE storage_dataset (
    storage_type_id bigint NOT NULL,
    storage_id bigint NOT NULL,
    storage_version integer NOT NULL,
    dataset_type_id bigint NOT NULL,
    dataset_id bigint NOT NULL
);


ALTER TABLE public.storage_dataset OWNER TO cube_admin;

--
-- TOC entry 4671 (class 0 OID 0)
-- Dependencies: 248
-- Name: TABLE storage_dataset; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE storage_dataset IS 'Data: Association between storage and dataset instances (many-many)';


--
-- TOC entry 249 (class 1259 OID 2486067)
-- Name: storage_dimension; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE storage_dimension (
    storage_type_id bigint NOT NULL,
    storage_id bigint NOT NULL,
    storage_version integer NOT NULL,
    domain_id bigint NOT NULL,
    dimension_id bigint NOT NULL,
    storage_dimension_index integer NOT NULL,
    storage_dimension_min double precision,
    storage_dimension_max double precision
);


ALTER TABLE public.storage_dimension OWNER TO cube_admin;

--
-- TOC entry 4673 (class 0 OID 0)
-- Dependencies: 249
-- Name: TABLE storage_dimension; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE storage_dimension IS 'Data: Association between storage and dimensions. Used to define attributes for each dimension in storage instances';


--
-- TOC entry 250 (class 1259 OID 2486079)
-- Name: storage_id_seq; Type: SEQUENCE; Schema: public; Owner: cube_admin
--

CREATE SEQUENCE storage_id_seq
    START WITH 100
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.storage_id_seq OWNER TO cube_admin;

--
-- TOC entry 251 (class 1259 OID 2486081)
-- Name: storage_type_measurement_type; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE storage_type_measurement_type (
    storage_type_id bigint NOT NULL,
    measurement_metatype_id bigint NOT NULL,
    measurement_type_id bigint NOT NULL,
    datatype_id smallint,
    measurement_type_index smallint
);


ALTER TABLE public.storage_type_measurement_type OWNER TO cube_admin;

--
-- TOC entry 4675 (class 0 OID 0)
-- Dependencies: 251
-- Name: TABLE storage_type_measurement_type; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE storage_type_measurement_type IS 'Configuration: Associations between n-dimensional data structure types and measurement types (i.e. variables) (many-many)';


SET search_path = earth_observation, pg_catalog;

--
-- TOC entry 4305 (class 2606 OID 2486097)
-- Name: pk_spectral_parameters; Type: CONSTRAINT; Schema: earth_observation; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY spectral_parameters
    ADD CONSTRAINT pk_spectral_parameters PRIMARY KEY (measurement_metatype_id, measurement_type_id);


SET search_path = public, pg_catalog;

--
-- TOC entry 4309 (class 2606 OID 2486099)
-- Name: pk_dataset; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dataset
    ADD CONSTRAINT pk_dataset PRIMARY KEY (dataset_type_id, dataset_id);


--
-- TOC entry 4313 (class 2606 OID 2486101)
-- Name: pk_dataset_dimension; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dataset_dimension
    ADD CONSTRAINT pk_dataset_dimension PRIMARY KEY (dataset_type_id, dataset_id, domain_id, dimension_id);


--
-- TOC entry 4316 (class 2606 OID 2486103)
-- Name: pk_dataset_metadata; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dataset_metadata
    ADD CONSTRAINT pk_dataset_metadata PRIMARY KEY (dataset_type_id, dataset_id);


--
-- TOC entry 4318 (class 2606 OID 2486105)
-- Name: pk_dataset_type; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dataset_type
    ADD CONSTRAINT pk_dataset_type PRIMARY KEY (dataset_type_id);


--
-- TOC entry 4324 (class 2606 OID 2486107)
-- Name: pk_dataset_type_dimension; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dataset_type_dimension
    ADD CONSTRAINT pk_dataset_type_dimension PRIMARY KEY (dataset_type_id, domain_id, dimension_id);


--
-- TOC entry 4329 (class 2606 OID 2486109)
-- Name: pk_dataset_type_domain; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dataset_type_domain
    ADD CONSTRAINT pk_dataset_type_domain PRIMARY KEY (dataset_type_id, domain_id);


--
-- TOC entry 4334 (class 2606 OID 2486111)
-- Name: pk_dataset_type_measurement_type; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dataset_type_measurement_type
    ADD CONSTRAINT pk_dataset_type_measurement_type PRIMARY KEY (dataset_type_id, measurement_metatype_id, measurement_type_id);


--
-- TOC entry 4338 (class 2606 OID 2486113)
-- Name: pk_datatype; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY datatype
    ADD CONSTRAINT pk_datatype PRIMARY KEY (datatype_id);


--
-- TOC entry 4342 (class 2606 OID 2486115)
-- Name: pk_dimension; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dimension
    ADD CONSTRAINT pk_dimension PRIMARY KEY (dimension_id);


--
-- TOC entry 4350 (class 2606 OID 2486117)
-- Name: pk_dimension_domain; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dimension_domain
    ADD CONSTRAINT pk_dimension_domain PRIMARY KEY (domain_id, dimension_id);


--
-- TOC entry 4352 (class 2606 OID 2486119)
-- Name: pk_domain; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY domain
    ADD CONSTRAINT pk_domain PRIMARY KEY (domain_id);


--
-- TOC entry 4356 (class 2606 OID 2486121)
-- Name: pk_indexing_type; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY indexing_type
    ADD CONSTRAINT pk_indexing_type PRIMARY KEY (indexing_type_id);


--
-- TOC entry 4389 (class 2606 OID 2486123)
-- Name: pk_instrument; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY instrument
    ADD CONSTRAINT pk_instrument PRIMARY KEY (instrument_type_id, instrument_id);


--
-- TOC entry 4393 (class 2606 OID 2486125)
-- Name: pk_instrument_type; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY instrument_type
    ADD CONSTRAINT pk_instrument_type PRIMARY KEY (instrument_type_id);


--
-- TOC entry 4397 (class 2606 OID 2486127)
-- Name: pk_measurement_metatype; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY measurement_metatype
    ADD CONSTRAINT pk_measurement_metatype PRIMARY KEY (measurement_metatype_id);


--
-- TOC entry 4402 (class 2606 OID 2486129)
-- Name: pk_measurement_type; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY measurement_type
    ADD CONSTRAINT pk_measurement_type PRIMARY KEY (measurement_metatype_id, measurement_type_id);


--
-- TOC entry 4431 (class 2606 OID 2486149)
-- Name: pk_observation; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY observation
    ADD CONSTRAINT pk_observation PRIMARY KEY (observation_type_id, observation_id);


--
-- TOC entry 4433 (class 2606 OID 2486151)
-- Name: pk_observation_type; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY observation_type
    ADD CONSTRAINT pk_observation_type PRIMARY KEY (observation_type_id);


--
-- TOC entry 4438 (class 2606 OID 2486153)
-- Name: pk_platform; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY platform
    ADD CONSTRAINT pk_platform PRIMARY KEY (platform_type_id, platform_id);


--
-- TOC entry 4442 (class 2606 OID 2486155)
-- Name: pk_platform_type; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY platform_type
    ADD CONSTRAINT pk_platform_type PRIMARY KEY (platform_type_id);


--
-- TOC entry 4371 (class 2606 OID 2486157)
-- Name: pk_reference_system; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY reference_system
    ADD CONSTRAINT pk_reference_system PRIMARY KEY (reference_system_id);


--
-- TOC entry 4377 (class 2606 OID 2486159)
-- Name: pk_reference_system_indexing; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY reference_system_indexing
    ADD CONSTRAINT pk_reference_system_indexing PRIMARY KEY (reference_system_id, array_index);


--
-- TOC entry 4446 (class 2606 OID 2486536)
-- Name: pk_spatial_footprint; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY spatial_footprint
    ADD CONSTRAINT pk_spatial_footprint PRIMARY KEY (spatial_footprint_id);


--
-- TOC entry 4407 (class 2606 OID 2486131)
-- Name: pk_storage; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY storage
    ADD CONSTRAINT pk_storage PRIMARY KEY (storage_type_id, storage_id, storage_version);


--
-- TOC entry 4413 (class 2606 OID 2486133)
-- Name: pk_storage_dataset; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY storage_dataset
    ADD CONSTRAINT pk_storage_dataset PRIMARY KEY (storage_type_id, storage_id, storage_version, dataset_type_id, dataset_id);


--
-- TOC entry 4420 (class 2606 OID 2486135)
-- Name: pk_storage_dimension; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY storage_dimension
    ADD CONSTRAINT pk_storage_dimension PRIMARY KEY (storage_type_id, storage_id, storage_version, domain_id, dimension_id);


--
-- TOC entry 4425 (class 2606 OID 2486147)
-- Name: pk_storage_measurement_type; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY storage_type_measurement_type
    ADD CONSTRAINT pk_storage_measurement_type PRIMARY KEY (storage_type_id, measurement_metatype_id, measurement_type_id);


--
-- TOC entry 4360 (class 2606 OID 2486141)
-- Name: pk_storage_type; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY storage_type
    ADD CONSTRAINT pk_storage_type PRIMARY KEY (storage_type_id);


--
-- TOC entry 4367 (class 2606 OID 2486143)
-- Name: pk_storage_type_dimension; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY storage_type_dimension
    ADD CONSTRAINT pk_storage_type_dimension PRIMARY KEY (storage_type_id, domain_id, dimension_id);


--
-- TOC entry 4381 (class 2606 OID 2486145)
-- Name: pk_storage_type_dimension_property; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY storage_type_dimension_property
    ADD CONSTRAINT pk_storage_type_dimension_property PRIMARY KEY (storage_type_id, domain_id, dimension_id, property_id);


--
-- TOC entry 4383 (class 2606 OID 2486161)
-- Name: property_pkey; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY property
    ADD CONSTRAINT property_pkey PRIMARY KEY (property_id);


--
-- TOC entry 4320 (class 2606 OID 2486163)
-- Name: uq_dataset_type_dataset_type_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dataset_type
    ADD CONSTRAINT uq_dataset_type_dataset_type_name UNIQUE (dataset_type_name);


--
-- TOC entry 4336 (class 2606 OID 2486165)
-- Name: uq_dataset_type_measurement_type_dataset_type; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dataset_type_measurement_type
    ADD CONSTRAINT uq_dataset_type_measurement_type_dataset_type UNIQUE (dataset_type_id, measurement_type_index);


--
-- TOC entry 4340 (class 2606 OID 2486167)
-- Name: uq_datatype_datatype_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY datatype
    ADD CONSTRAINT uq_datatype_datatype_name UNIQUE (datatype_name);


--
-- TOC entry 4344 (class 2606 OID 2486169)
-- Name: uq_dimension_dimension_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dimension
    ADD CONSTRAINT uq_dimension_dimension_name UNIQUE (dimension_name);


--
-- TOC entry 4346 (class 2606 OID 2486171)
-- Name: uq_dimension_dimension_tag; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dimension
    ADD CONSTRAINT uq_dimension_dimension_tag UNIQUE (dimension_tag);


--
-- TOC entry 4354 (class 2606 OID 2486173)
-- Name: uq_domain_domain_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY domain
    ADD CONSTRAINT uq_domain_domain_name UNIQUE (domain_name);


--
-- TOC entry 4358 (class 2606 OID 2486175)
-- Name: uq_indexing_type_indexing_type_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY indexing_type
    ADD CONSTRAINT uq_indexing_type_indexing_type_name UNIQUE (indexing_type_name);


--
-- TOC entry 4391 (class 2606 OID 2486177)
-- Name: uq_instrument_instrument_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY instrument
    ADD CONSTRAINT uq_instrument_instrument_name UNIQUE (instrument_name);


--
-- TOC entry 4395 (class 2606 OID 2486179)
-- Name: uq_instrument_type_instrument_type_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY instrument_type
    ADD CONSTRAINT uq_instrument_type_instrument_type_name UNIQUE (instrument_type_name);


--
-- TOC entry 4399 (class 2606 OID 2486181)
-- Name: uq_measurement_metatype_measurement_metatype_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY measurement_metatype
    ADD CONSTRAINT uq_measurement_metatype_measurement_metatype_name UNIQUE (measurement_metatype_name);


--
-- TOC entry 4435 (class 2606 OID 2486191)
-- Name: uq_observation_type_observation_type_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY observation_type
    ADD CONSTRAINT uq_observation_type_observation_type_name UNIQUE (observation_type_name);


--
-- TOC entry 4440 (class 2606 OID 2486193)
-- Name: uq_platform_platform_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY platform
    ADD CONSTRAINT uq_platform_platform_name UNIQUE (platform_name);


--
-- TOC entry 4444 (class 2606 OID 2486195)
-- Name: uq_platform_type_platform_type_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY platform_type
    ADD CONSTRAINT uq_platform_type_platform_type_name UNIQUE (platform_type_name);


--
-- TOC entry 4385 (class 2606 OID 2486197)
-- Name: uq_property_property_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY property
    ADD CONSTRAINT uq_property_property_name UNIQUE (property_name);


--
-- TOC entry 4373 (class 2606 OID 2486199)
-- Name: uq_reference_system_reference_system_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY reference_system
    ADD CONSTRAINT uq_reference_system_reference_system_name UNIQUE (reference_system_name);


--
-- TOC entry 4409 (class 2606 OID 2486183)
-- Name: uq_storage_storage_location; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY storage
    ADD CONSTRAINT uq_storage_storage_location UNIQUE (storage_location);


--
-- TOC entry 4369 (class 2606 OID 2486185)
-- Name: uq_storage_type_dimension_storage_type_dimension; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY storage_type_dimension
    ADD CONSTRAINT uq_storage_type_dimension_storage_type_dimension UNIQUE (storage_type_id, dimension_id);


--
-- TOC entry 4677 (class 0 OID 0)
-- Dependencies: 4369
-- Name: CONSTRAINT uq_storage_type_dimension_storage_type_dimension ON storage_type_dimension; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON CONSTRAINT uq_storage_type_dimension_storage_type_dimension ON storage_type_dimension IS 'Unique constraint to ensure each dimension is only represented once in each storage_type';


--
-- TOC entry 4427 (class 2606 OID 2486187)
-- Name: uq_storage_type_measurement_type_storage_type_id_measurement_ty; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY storage_type_measurement_type
    ADD CONSTRAINT uq_storage_type_measurement_type_storage_type_id_measurement_ty UNIQUE (storage_type_id, measurement_type_index);


--
-- TOC entry 4362 (class 2606 OID 2486189)
-- Name: uq_storage_type_storage_type_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY storage_type
    ADD CONSTRAINT uq_storage_type_storage_type_name UNIQUE (storage_type_name);


SET search_path = earth_observation, pg_catalog;

--
-- TOC entry 4303 (class 1259 OID 2486200)
-- Name: fki_spectral_parameters_measurement_type; Type: INDEX; Schema: earth_observation; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_spectral_parameters_measurement_type ON spectral_parameters USING btree (measurement_metatype_id, measurement_type_id);


SET search_path = public, pg_catalog;

--
-- TOC entry 4306 (class 1259 OID 2486201)
-- Name: fki_dataset_dataset_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_dataset_type ON dataset USING btree (dataset_type_id);


--
-- TOC entry 4310 (class 1259 OID 2486202)
-- Name: fki_dataset_dimension_dataset; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_dimension_dataset ON dataset_dimension USING btree (dataset_type_id, dataset_id);


--
-- TOC entry 4311 (class 1259 OID 2486203)
-- Name: fki_dataset_dimension_dataset_type_dimension; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_dimension_dataset_type_dimension ON dataset_dimension USING btree (dataset_type_id, domain_id, dimension_id);


--
-- TOC entry 4314 (class 1259 OID 2486204)
-- Name: fki_dataset_metadata_dataset; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_metadata_dataset ON dataset_metadata USING btree (dataset_type_id, dataset_id);


--
-- TOC entry 4307 (class 1259 OID 2486205)
-- Name: fki_dataset_observation; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_observation ON dataset USING btree (observation_type_id, observation_id);


--
-- TOC entry 4321 (class 1259 OID 2486206)
-- Name: fki_dataset_type_dimension_dataset_type_domain; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_type_dimension_dataset_type_domain ON dataset_type_dimension USING btree (dataset_type_id, domain_id);


--
-- TOC entry 4322 (class 1259 OID 2486207)
-- Name: fki_dataset_type_dimension_dimension_domain; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_type_dimension_dimension_domain ON dataset_type_dimension USING btree (domain_id, dimension_id);


--
-- TOC entry 4325 (class 1259 OID 2486208)
-- Name: fki_dataset_type_domain_dataset_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_type_domain_dataset_type ON dataset_type_domain USING btree (dataset_type_id);


--
-- TOC entry 4326 (class 1259 OID 2486209)
-- Name: fki_dataset_type_domain_domain; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_type_domain_domain ON dataset_type_domain USING btree (domain_id);


--
-- TOC entry 4327 (class 1259 OID 2486210)
-- Name: fki_dataset_type_domain_reference_system; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_type_domain_reference_system ON dataset_type_domain USING btree (reference_system_id);


--
-- TOC entry 4330 (class 1259 OID 2486211)
-- Name: fki_dataset_type_measurement_metatype_datatype; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_type_measurement_metatype_datatype ON dataset_type_measurement_type USING btree (datatype_id);


--
-- TOC entry 4331 (class 1259 OID 2486212)
-- Name: fki_dataset_type_measurement_type_dataset_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_type_measurement_type_dataset_type ON dataset_type_measurement_type USING btree (dataset_type_id);


--
-- TOC entry 4332 (class 1259 OID 2486213)
-- Name: fki_dataset_type_measurement_type_measurement_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_type_measurement_type_measurement_type ON dataset_type_measurement_type USING btree (measurement_metatype_id, measurement_type_id);


--
-- TOC entry 4347 (class 1259 OID 2486214)
-- Name: fki_dimension_domain_dimension; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dimension_domain_dimension ON dimension_domain USING btree (dimension_id);


--
-- TOC entry 4348 (class 1259 OID 2486215)
-- Name: fki_dimension_domain_domain; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dimension_domain_domain ON dimension_domain USING btree (domain_id);


--
-- TOC entry 4386 (class 1259 OID 2486216)
-- Name: fki_instrument_instrument_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_instrument_instrument_type ON instrument USING btree (instrument_type_id);


--
-- TOC entry 4387 (class 1259 OID 2486217)
-- Name: fki_instrument_platform; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_instrument_platform ON instrument USING btree (platform_type_id, platform_id);


--
-- TOC entry 4400 (class 1259 OID 2486218)
-- Name: fki_measurement_type_measurement_metatype; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_measurement_type_measurement_metatype ON measurement_type USING btree (measurement_metatype_id);


--
-- TOC entry 4403 (class 1259 OID 2486225)
-- Name: fki_ndarray_footprint_ndarray_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_ndarray_footprint_ndarray_type ON storage USING btree (storage_type_id);


--
-- TOC entry 4428 (class 1259 OID 2486235)
-- Name: fki_observation_instrument; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_observation_instrument ON observation USING btree (instrument_type_id, instrument_id);


--
-- TOC entry 4429 (class 1259 OID 2486236)
-- Name: fki_observation_observation_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_observation_observation_type ON observation USING btree (observation_type_id);


--
-- TOC entry 4436 (class 1259 OID 2486237)
-- Name: fki_platform_platform_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_platform_platform_type ON platform USING btree (platform_type_id);


--
-- TOC entry 4374 (class 1259 OID 2486238)
-- Name: fki_reference_system_indexing_measurement_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_reference_system_indexing_measurement_type ON reference_system_indexing USING btree (measurement_metatype_id, measurement_type_id);


--
-- TOC entry 4375 (class 1259 OID 2486239)
-- Name: fki_reference_system_indexing_reference_system; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_reference_system_indexing_reference_system ON reference_system_indexing USING btree (reference_system_id);


--
-- TOC entry 4410 (class 1259 OID 2486219)
-- Name: fki_storage_dataset_dataset; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_storage_dataset_dataset ON storage_dataset USING btree (dataset_type_id, dataset_id);


--
-- TOC entry 4411 (class 1259 OID 2486220)
-- Name: fki_storage_dataset_storage; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_storage_dataset_storage ON storage_dataset USING btree (storage_type_id, storage_id, storage_version);


--
-- TOC entry 4414 (class 1259 OID 2486221)
-- Name: fki_storage_dimension_storage; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_storage_dimension_storage ON storage_dimension USING btree (storage_type_id, storage_id, storage_version);


--
-- TOC entry 4415 (class 1259 OID 2486222)
-- Name: fki_storage_dimension_storage_type_dimension; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_storage_dimension_storage_type_dimension ON storage_dimension USING btree (storage_type_id, domain_id, dimension_id);


--
-- TOC entry 4404 (class 1259 OID 2486542)
-- Name: fki_storage_spatial_footprint; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_storage_spatial_footprint ON storage USING btree (spatial_footprint_id);


--
-- TOC entry 4405 (class 1259 OID 2486226)
-- Name: fki_storage_storage_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_storage_storage_type ON storage USING btree (storage_type_id, storage_type_id);


--
-- TOC entry 4378 (class 1259 OID 2486228)
-- Name: fki_storage_type_dimension_attribute_property; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_storage_type_dimension_attribute_property ON storage_type_dimension_property USING btree (property_id);


--
-- TOC entry 4379 (class 1259 OID 2486227)
-- Name: fki_storage_type_dimension_attribute_storage_type_dimension; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_storage_type_dimension_attribute_storage_type_dimension ON storage_type_dimension_property USING btree (storage_type_id, domain_id, dimension_id);


--
-- TOC entry 4363 (class 1259 OID 2486229)
-- Name: fki_storage_type_dimension_dimension_domain; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_storage_type_dimension_dimension_domain ON storage_type_dimension USING btree (domain_id, dimension_id);


--
-- TOC entry 4364 (class 1259 OID 2486230)
-- Name: fki_storage_type_dimension_indexing_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_storage_type_dimension_indexing_type ON storage_type_dimension USING btree (indexing_type_id);


--
-- TOC entry 4365 (class 1259 OID 2486231)
-- Name: fki_storage_type_dimension_storage_type_id_fkey; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_storage_type_dimension_storage_type_id_fkey ON storage_type_dimension USING btree (storage_type_id, domain_id);


--
-- TOC entry 4421 (class 1259 OID 2486232)
-- Name: fki_storage_type_masurement_type_datatype; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_storage_type_masurement_type_datatype ON storage_type_measurement_type USING btree (datatype_id);


--
-- TOC entry 4422 (class 1259 OID 2486233)
-- Name: fki_storage_type_measurement_type_measurement_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_storage_type_measurement_type_measurement_type ON storage_type_measurement_type USING btree (measurement_metatype_id, measurement_type_id);


--
-- TOC entry 4423 (class 1259 OID 2486234)
-- Name: fki_storage_type_measurement_type_storage_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_storage_type_measurement_type_storage_type ON storage_type_measurement_type USING btree (storage_type_id);


--
-- TOC entry 4416 (class 1259 OID 2486240)
-- Name: idx_storage_dimension_storage_dimension_index; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX idx_storage_dimension_storage_dimension_index ON storage_dimension USING btree (storage_dimension_index);


--
-- TOC entry 4417 (class 1259 OID 2486241)
-- Name: idx_storage_dimension_storage_dimension_max; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX idx_storage_dimension_storage_dimension_max ON storage_dimension USING btree (storage_dimension_max);


--
-- TOC entry 4418 (class 1259 OID 2486242)
-- Name: idx_storage_dimension_storage_dimension_min; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX idx_storage_dimension_storage_dimension_min ON storage_dimension USING btree (storage_dimension_max);


SET search_path = earth_observation, pg_catalog;

--
-- TOC entry 4447 (class 2606 OID 2486243)
-- Name: fk_spectral_parameters_measurement_metatype; Type: FK CONSTRAINT; Schema: earth_observation; Owner: cube_admin
--

ALTER TABLE ONLY spectral_parameters
    ADD CONSTRAINT fk_spectral_parameters_measurement_metatype FOREIGN KEY (measurement_metatype_id, measurement_type_id) REFERENCES public.measurement_type(measurement_metatype_id, measurement_type_id);


SET search_path = public, pg_catalog;

--
-- TOC entry 4448 (class 2606 OID 2486248)
-- Name: fk_dataset_dataset_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset
    ADD CONSTRAINT fk_dataset_dataset_type FOREIGN KEY (dataset_type_id) REFERENCES dataset_type(dataset_type_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4450 (class 2606 OID 2486253)
-- Name: fk_dataset_dimension_dataset; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset_dimension
    ADD CONSTRAINT fk_dataset_dimension_dataset FOREIGN KEY (dataset_type_id, dataset_id) REFERENCES dataset(dataset_type_id, dataset_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4451 (class 2606 OID 2486258)
-- Name: fk_dataset_dimension_dataset_type_dimension; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset_dimension
    ADD CONSTRAINT fk_dataset_dimension_dataset_type_dimension FOREIGN KEY (dataset_type_id, domain_id, dimension_id) REFERENCES dataset_type_dimension(dataset_type_id, domain_id, dimension_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4452 (class 2606 OID 2486263)
-- Name: fk_dataset_metadata_dataset; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset_metadata
    ADD CONSTRAINT fk_dataset_metadata_dataset FOREIGN KEY (dataset_type_id, dataset_id) REFERENCES dataset(dataset_type_id, dataset_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4449 (class 2606 OID 2486268)
-- Name: fk_dataset_observation; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset
    ADD CONSTRAINT fk_dataset_observation FOREIGN KEY (observation_type_id, observation_id) REFERENCES observation(observation_type_id, observation_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4453 (class 2606 OID 2486273)
-- Name: fk_dataset_type_dimension_dataset_type_domain; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset_type_dimension
    ADD CONSTRAINT fk_dataset_type_dimension_dataset_type_domain FOREIGN KEY (dataset_type_id, domain_id) REFERENCES dataset_type_domain(dataset_type_id, domain_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4454 (class 2606 OID 2486278)
-- Name: fk_dataset_type_dimension_dimension_domain; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset_type_dimension
    ADD CONSTRAINT fk_dataset_type_dimension_dimension_domain FOREIGN KEY (domain_id, dimension_id) REFERENCES dimension_domain(domain_id, dimension_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4455 (class 2606 OID 2486283)
-- Name: fk_dataset_type_domain_dataset_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset_type_domain
    ADD CONSTRAINT fk_dataset_type_domain_dataset_type FOREIGN KEY (dataset_type_id) REFERENCES dataset_type(dataset_type_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4456 (class 2606 OID 2486288)
-- Name: fk_dataset_type_domain_domain; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset_type_domain
    ADD CONSTRAINT fk_dataset_type_domain_domain FOREIGN KEY (domain_id) REFERENCES domain(domain_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4457 (class 2606 OID 2486293)
-- Name: fk_dataset_type_domain_reference_system; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset_type_domain
    ADD CONSTRAINT fk_dataset_type_domain_reference_system FOREIGN KEY (reference_system_id) REFERENCES reference_system(reference_system_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4458 (class 2606 OID 2486298)
-- Name: fk_dataset_type_measurement_type_dataset_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset_type_measurement_type
    ADD CONSTRAINT fk_dataset_type_measurement_type_dataset_type FOREIGN KEY (dataset_type_id) REFERENCES dataset_type(dataset_type_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4459 (class 2606 OID 2486303)
-- Name: fk_dataset_type_measurement_type_datatype; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset_type_measurement_type
    ADD CONSTRAINT fk_dataset_type_measurement_type_datatype FOREIGN KEY (datatype_id) REFERENCES datatype(datatype_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4460 (class 2606 OID 2486308)
-- Name: fk_dataset_type_measurement_type_measurement_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset_type_measurement_type
    ADD CONSTRAINT fk_dataset_type_measurement_type_measurement_type FOREIGN KEY (measurement_metatype_id, measurement_type_id) REFERENCES measurement_type(measurement_metatype_id, measurement_type_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4461 (class 2606 OID 2486313)
-- Name: fk_dimension_domain_dimension; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dimension_domain
    ADD CONSTRAINT fk_dimension_domain_dimension FOREIGN KEY (dimension_id) REFERENCES dimension(dimension_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4462 (class 2606 OID 2486318)
-- Name: fk_dimension_domain_domain; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dimension_domain
    ADD CONSTRAINT fk_dimension_domain_domain FOREIGN KEY (domain_id) REFERENCES domain(domain_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4471 (class 2606 OID 2486323)
-- Name: fk_instrument_instrument_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY instrument
    ADD CONSTRAINT fk_instrument_instrument_type FOREIGN KEY (instrument_type_id) REFERENCES instrument_type(instrument_type_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4472 (class 2606 OID 2486328)
-- Name: fk_instrument_platform; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY instrument
    ADD CONSTRAINT fk_instrument_platform FOREIGN KEY (platform_type_id, platform_id) REFERENCES platform(platform_type_id, platform_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4473 (class 2606 OID 2486333)
-- Name: fk_measurement_type_measurement_metatype; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY measurement_type
    ADD CONSTRAINT fk_measurement_type_measurement_metatype FOREIGN KEY (measurement_metatype_id) REFERENCES measurement_metatype(measurement_metatype_id);


--
-- TOC entry 4483 (class 2606 OID 2486423)
-- Name: fk_observation_instrument; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY observation
    ADD CONSTRAINT fk_observation_instrument FOREIGN KEY (instrument_type_id, instrument_id) REFERENCES instrument(instrument_type_id, instrument_id);


--
-- TOC entry 4484 (class 2606 OID 2486428)
-- Name: fk_observation_observation_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY observation
    ADD CONSTRAINT fk_observation_observation_type FOREIGN KEY (observation_type_id) REFERENCES observation_type(observation_type_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4485 (class 2606 OID 2486433)
-- Name: fk_platform_platform_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY platform
    ADD CONSTRAINT fk_platform_platform_type FOREIGN KEY (platform_type_id) REFERENCES platform_type(platform_type_id);


--
-- TOC entry 4467 (class 2606 OID 2486438)
-- Name: fk_reference_system_indexing_measurement_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY reference_system_indexing
    ADD CONSTRAINT fk_reference_system_indexing_measurement_type FOREIGN KEY (measurement_metatype_id, measurement_type_id) REFERENCES measurement_type(measurement_metatype_id, measurement_type_id);


--
-- TOC entry 4468 (class 2606 OID 2486443)
-- Name: fk_reference_system_indexing_reference_system; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY reference_system_indexing
    ADD CONSTRAINT fk_reference_system_indexing_reference_system FOREIGN KEY (reference_system_id) REFERENCES reference_system(reference_system_id);


--
-- TOC entry 4476 (class 2606 OID 2486338)
-- Name: fk_storage_dataset_dataset; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY storage_dataset
    ADD CONSTRAINT fk_storage_dataset_dataset FOREIGN KEY (dataset_type_id, dataset_id) REFERENCES dataset(dataset_type_id, dataset_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4477 (class 2606 OID 2486343)
-- Name: fk_storage_dataset_storage; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY storage_dataset
    ADD CONSTRAINT fk_storage_dataset_storage FOREIGN KEY (storage_type_id, storage_id, storage_version) REFERENCES storage(storage_type_id, storage_id, storage_version) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4478 (class 2606 OID 2486348)
-- Name: fk_storage_dimension_storage; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY storage_dimension
    ADD CONSTRAINT fk_storage_dimension_storage FOREIGN KEY (storage_type_id, storage_id, storage_version) REFERENCES storage(storage_type_id, storage_id, storage_version) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4479 (class 2606 OID 2486353)
-- Name: fk_storage_dimension_storage_type_dimension; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY storage_dimension
    ADD CONSTRAINT fk_storage_dimension_storage_type_dimension FOREIGN KEY (storage_type_id, domain_id, dimension_id) REFERENCES storage_type_dimension(storage_type_id, domain_id, dimension_id);


--
-- TOC entry 4474 (class 2606 OID 2486537)
-- Name: fk_storage_spatial_footprint; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY storage
    ADD CONSTRAINT fk_storage_spatial_footprint FOREIGN KEY (spatial_footprint_id) REFERENCES spatial_footprint(spatial_footprint_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4475 (class 2606 OID 2486373)
-- Name: fk_storage_storage_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY storage
    ADD CONSTRAINT fk_storage_storage_type FOREIGN KEY (storage_type_id) REFERENCES storage_type(storage_type_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4470 (class 2606 OID 2486378)
-- Name: fk_storage_type_dimension_attribute_storage_type_dimension; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY storage_type_dimension_property
    ADD CONSTRAINT fk_storage_type_dimension_attribute_storage_type_dimension FOREIGN KEY (storage_type_id, domain_id, dimension_id) REFERENCES storage_type_dimension(storage_type_id, domain_id, dimension_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4463 (class 2606 OID 2486383)
-- Name: fk_storage_type_dimension_dimension_domain; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY storage_type_dimension
    ADD CONSTRAINT fk_storage_type_dimension_dimension_domain FOREIGN KEY (domain_id, dimension_id) REFERENCES dimension_domain(domain_id, dimension_id) ON UPDATE CASCADE;


--
-- TOC entry 4464 (class 2606 OID 2486388)
-- Name: fk_storage_type_dimension_indexing_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY storage_type_dimension
    ADD CONSTRAINT fk_storage_type_dimension_indexing_type FOREIGN KEY (indexing_type_id) REFERENCES indexing_type(indexing_type_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4469 (class 2606 OID 2486398)
-- Name: fk_storage_type_dimension_property_property; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY storage_type_dimension_property
    ADD CONSTRAINT fk_storage_type_dimension_property_property FOREIGN KEY (property_id) REFERENCES property(property_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4466 (class 2606 OID 2486403)
-- Name: fk_storage_type_dimension_reference_system; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY storage_type_dimension
    ADD CONSTRAINT fk_storage_type_dimension_reference_system FOREIGN KEY (reference_system_id) REFERENCES reference_system(reference_system_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4465 (class 2606 OID 2486393)
-- Name: fk_storage_type_dimension_storage_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY storage_type_dimension
    ADD CONSTRAINT fk_storage_type_dimension_storage_type FOREIGN KEY (storage_type_id) REFERENCES storage_type(storage_type_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4480 (class 2606 OID 2486408)
-- Name: fk_storage_type_measurement_type_datatype; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY storage_type_measurement_type
    ADD CONSTRAINT fk_storage_type_measurement_type_datatype FOREIGN KEY (datatype_id) REFERENCES datatype(datatype_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4481 (class 2606 OID 2486413)
-- Name: fk_storage_type_measurement_type_measurement_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY storage_type_measurement_type
    ADD CONSTRAINT fk_storage_type_measurement_type_measurement_type FOREIGN KEY (measurement_metatype_id, measurement_type_id) REFERENCES measurement_type(measurement_metatype_id, measurement_type_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4482 (class 2606 OID 2486418)
-- Name: fk_storage_type_measurement_type_storage_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY storage_type_measurement_type
    ADD CONSTRAINT fk_storage_type_measurement_type_storage_type FOREIGN KEY (storage_type_id) REFERENCES storage_type(storage_type_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4608 (class 0 OID 0)
-- Dependencies: 9
-- Name: public; Type: ACL; Schema: -; Owner: cube_admin
--

REVOKE ALL ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM cube_admin;
GRANT ALL ON SCHEMA public TO cube_admin;
GRANT ALL ON SCHEMA public TO postgres;
GRANT ALL ON SCHEMA public TO PUBLIC;


--
-- TOC entry 4610 (class 0 OID 0)
-- Dependencies: 10
-- Name: ztmp; Type: ACL; Schema: -; Owner: postgres
--

REVOKE ALL ON SCHEMA ztmp FROM PUBLIC;
REVOKE ALL ON SCHEMA ztmp FROM postgres;
GRANT ALL ON SCHEMA ztmp TO postgres;
GRANT ALL ON SCHEMA ztmp TO PUBLIC;


SET search_path = earth_observation, pg_catalog;

--
-- TOC entry 4612 (class 0 OID 0)
-- Dependencies: 224
-- Name: spectral_parameters; Type: ACL; Schema: earth_observation; Owner: cube_admin
--

REVOKE ALL ON TABLE spectral_parameters FROM PUBLIC;
REVOKE ALL ON TABLE spectral_parameters FROM cube_admin;
GRANT ALL ON TABLE spectral_parameters TO cube_admin;
GRANT ALL ON TABLE spectral_parameters TO cube_admin_group;
GRANT SELECT ON TABLE spectral_parameters TO cube_user_group;


SET search_path = public, pg_catalog;

--
-- TOC entry 4614 (class 0 OID 0)
-- Dependencies: 225
-- Name: dataset; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE dataset FROM PUBLIC;
REVOKE ALL ON TABLE dataset FROM cube_admin;
GRANT ALL ON TABLE dataset TO cube_admin;
GRANT ALL ON TABLE dataset TO cube_admin_group;
GRANT SELECT ON TABLE dataset TO cube_user_group;


--
-- TOC entry 4616 (class 0 OID 0)
-- Dependencies: 226
-- Name: dataset_dimension; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE dataset_dimension FROM PUBLIC;
REVOKE ALL ON TABLE dataset_dimension FROM cube_admin;
GRANT ALL ON TABLE dataset_dimension TO cube_admin;
GRANT ALL ON TABLE dataset_dimension TO cube_admin_group;
GRANT SELECT ON TABLE dataset_dimension TO cube_user_group;


--
-- TOC entry 4618 (class 0 OID 0)
-- Dependencies: 227
-- Name: dataset_metadata; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE dataset_metadata FROM PUBLIC;
REVOKE ALL ON TABLE dataset_metadata FROM cube_admin;
GRANT ALL ON TABLE dataset_metadata TO cube_admin;
GRANT ALL ON TABLE dataset_metadata TO cube_admin_group;
GRANT SELECT ON TABLE dataset_metadata TO cube_user_group;


--
-- TOC entry 4620 (class 0 OID 0)
-- Dependencies: 228
-- Name: dataset_type; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE dataset_type FROM PUBLIC;
REVOKE ALL ON TABLE dataset_type FROM cube_admin;
GRANT ALL ON TABLE dataset_type TO cube_admin;
GRANT ALL ON TABLE dataset_type TO cube_admin_group;
GRANT SELECT ON TABLE dataset_type TO cube_user_group;


--
-- TOC entry 4622 (class 0 OID 0)
-- Dependencies: 229
-- Name: dataset_type_dimension; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE dataset_type_dimension FROM PUBLIC;
REVOKE ALL ON TABLE dataset_type_dimension FROM cube_admin;
GRANT ALL ON TABLE dataset_type_dimension TO cube_admin;
GRANT ALL ON TABLE dataset_type_dimension TO cube_admin_group;
GRANT SELECT ON TABLE dataset_type_dimension TO cube_user_group;


--
-- TOC entry 4624 (class 0 OID 0)
-- Dependencies: 230
-- Name: dataset_type_domain; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE dataset_type_domain FROM PUBLIC;
REVOKE ALL ON TABLE dataset_type_domain FROM cube_admin;
GRANT ALL ON TABLE dataset_type_domain TO cube_admin;
GRANT ALL ON TABLE dataset_type_domain TO cube_admin_group;
GRANT SELECT ON TABLE dataset_type_domain TO cube_user_group;


--
-- TOC entry 4626 (class 0 OID 0)
-- Dependencies: 231
-- Name: dataset_type_measurement_type; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE dataset_type_measurement_type FROM PUBLIC;
REVOKE ALL ON TABLE dataset_type_measurement_type FROM cube_admin;
GRANT ALL ON TABLE dataset_type_measurement_type TO cube_admin;
GRANT ALL ON TABLE dataset_type_measurement_type TO cube_admin_group;
GRANT SELECT ON TABLE dataset_type_measurement_type TO cube_user_group;


--
-- TOC entry 4628 (class 0 OID 0)
-- Dependencies: 232
-- Name: datatype; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE datatype FROM PUBLIC;
REVOKE ALL ON TABLE datatype FROM cube_admin;
GRANT ALL ON TABLE datatype TO cube_admin;
GRANT ALL ON TABLE datatype TO cube_admin_group;
GRANT SELECT ON TABLE datatype TO cube_user_group;


--
-- TOC entry 4630 (class 0 OID 0)
-- Dependencies: 233
-- Name: dimension; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE dimension FROM PUBLIC;
REVOKE ALL ON TABLE dimension FROM cube_admin;
GRANT ALL ON TABLE dimension TO cube_admin;
GRANT ALL ON TABLE dimension TO cube_admin_group;
GRANT SELECT ON TABLE dimension TO cube_user_group;


--
-- TOC entry 4632 (class 0 OID 0)
-- Dependencies: 234
-- Name: dimension_domain; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE dimension_domain FROM PUBLIC;
REVOKE ALL ON TABLE dimension_domain FROM cube_admin;
GRANT ALL ON TABLE dimension_domain TO cube_admin;
GRANT ALL ON TABLE dimension_domain TO cube_admin_group;
GRANT SELECT ON TABLE dimension_domain TO cube_user_group;


--
-- TOC entry 4634 (class 0 OID 0)
-- Dependencies: 235
-- Name: domain; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE domain FROM PUBLIC;
REVOKE ALL ON TABLE domain FROM cube_admin;
GRANT ALL ON TABLE domain TO cube_admin;
GRANT ALL ON TABLE domain TO cube_admin_group;
GRANT SELECT ON TABLE domain TO cube_user_group;


--
-- TOC entry 4636 (class 0 OID 0)
-- Dependencies: 236
-- Name: indexing_type; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE indexing_type FROM PUBLIC;
REVOKE ALL ON TABLE indexing_type FROM cube_admin;
GRANT ALL ON TABLE indexing_type TO cube_admin;
GRANT ALL ON TABLE indexing_type TO cube_admin_group;
GRANT SELECT ON TABLE indexing_type TO cube_user_group;


--
-- TOC entry 4638 (class 0 OID 0)
-- Dependencies: 239
-- Name: reference_system; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE reference_system FROM PUBLIC;
REVOKE ALL ON TABLE reference_system FROM cube_admin;
GRANT ALL ON TABLE reference_system TO cube_admin;
GRANT ALL ON TABLE reference_system TO cube_admin_group;
GRANT SELECT ON TABLE reference_system TO cube_user_group;


--
-- TOC entry 4640 (class 0 OID 0)
-- Dependencies: 240
-- Name: reference_system_indexing; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE reference_system_indexing FROM PUBLIC;
REVOKE ALL ON TABLE reference_system_indexing FROM cube_admin;
GRANT ALL ON TABLE reference_system_indexing TO cube_admin;
GRANT ALL ON TABLE reference_system_indexing TO cube_admin_group;
GRANT SELECT ON TABLE reference_system_indexing TO cube_user_group;


--
-- TOC entry 4642 (class 0 OID 0)
-- Dependencies: 237
-- Name: storage_type; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE storage_type FROM PUBLIC;
REVOKE ALL ON TABLE storage_type FROM cube_admin;
GRANT ALL ON TABLE storage_type TO cube_admin;
GRANT ALL ON TABLE storage_type TO cube_admin_group;
GRANT SELECT ON TABLE storage_type TO cube_user_group;


--
-- TOC entry 4644 (class 0 OID 0)
-- Dependencies: 238
-- Name: storage_type_dimension; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE storage_type_dimension FROM PUBLIC;
REVOKE ALL ON TABLE storage_type_dimension FROM cube_admin;
GRANT ALL ON TABLE storage_type_dimension TO cube_admin;
GRANT ALL ON TABLE storage_type_dimension TO cube_admin_group;
GRANT SELECT ON TABLE storage_type_dimension TO cube_user_group;


--
-- TOC entry 4645 (class 0 OID 0)
-- Dependencies: 257
-- Name: storage_type_dimension_view; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE storage_type_dimension_view FROM PUBLIC;
REVOKE ALL ON TABLE storage_type_dimension_view FROM cube_admin;
GRANT ALL ON TABLE storage_type_dimension_view TO cube_admin;
GRANT ALL ON TABLE storage_type_dimension_view TO cube_admin_group;
GRANT SELECT ON TABLE storage_type_dimension_view TO cube_user_group;


--
-- TOC entry 4646 (class 0 OID 0)
-- Dependencies: 258
-- Name: dimension_indices_view; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE dimension_indices_view FROM PUBLIC;
REVOKE ALL ON TABLE dimension_indices_view FROM cube_admin;
GRANT ALL ON TABLE dimension_indices_view TO cube_admin;
GRANT ALL ON TABLE dimension_indices_view TO cube_admin_group;
GRANT SELECT ON TABLE dimension_indices_view TO cube_user_group;


--
-- TOC entry 4648 (class 0 OID 0)
-- Dependencies: 242
-- Name: property; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE property FROM PUBLIC;
REVOKE ALL ON TABLE property FROM cube_admin;
GRANT ALL ON TABLE property TO cube_admin;
GRANT ALL ON TABLE property TO cube_admin_group;
GRANT SELECT ON TABLE property TO cube_user_group;


--
-- TOC entry 4650 (class 0 OID 0)
-- Dependencies: 241
-- Name: storage_type_dimension_property; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE storage_type_dimension_property FROM PUBLIC;
REVOKE ALL ON TABLE storage_type_dimension_property FROM cube_admin;
GRANT ALL ON TABLE storage_type_dimension_property TO cube_admin;
GRANT ALL ON TABLE storage_type_dimension_property TO cube_admin_group;
GRANT SELECT ON TABLE storage_type_dimension_property TO cube_user_group;


--
-- TOC entry 4651 (class 0 OID 0)
-- Dependencies: 259
-- Name: dimension_properties_view; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE dimension_properties_view FROM PUBLIC;
REVOKE ALL ON TABLE dimension_properties_view FROM cube_admin;
GRANT ALL ON TABLE dimension_properties_view TO cube_admin;
GRANT ALL ON TABLE dimension_properties_view TO cube_admin_group;
GRANT SELECT ON TABLE dimension_properties_view TO cube_user_group;


--
-- TOC entry 4653 (class 0 OID 0)
-- Dependencies: 243
-- Name: instrument; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE instrument FROM PUBLIC;
REVOKE ALL ON TABLE instrument FROM cube_admin;
GRANT ALL ON TABLE instrument TO cube_admin;
GRANT ALL ON TABLE instrument TO cube_admin_group;
GRANT SELECT ON TABLE instrument TO cube_user_group;


--
-- TOC entry 4655 (class 0 OID 0)
-- Dependencies: 244
-- Name: instrument_type; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE instrument_type FROM PUBLIC;
REVOKE ALL ON TABLE instrument_type FROM cube_admin;
GRANT ALL ON TABLE instrument_type TO cube_admin;
GRANT ALL ON TABLE instrument_type TO cube_admin_group;
GRANT SELECT ON TABLE instrument_type TO cube_user_group;


--
-- TOC entry 4657 (class 0 OID 0)
-- Dependencies: 245
-- Name: measurement_metatype; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE measurement_metatype FROM PUBLIC;
REVOKE ALL ON TABLE measurement_metatype FROM cube_admin;
GRANT ALL ON TABLE measurement_metatype TO cube_admin;
GRANT ALL ON TABLE measurement_metatype TO cube_admin_group;
GRANT SELECT ON TABLE measurement_metatype TO cube_user_group;


--
-- TOC entry 4659 (class 0 OID 0)
-- Dependencies: 246
-- Name: measurement_type; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE measurement_type FROM PUBLIC;
REVOKE ALL ON TABLE measurement_type FROM cube_admin;
GRANT ALL ON TABLE measurement_type TO cube_admin;
GRANT ALL ON TABLE measurement_type TO cube_admin_group;
GRANT SELECT ON TABLE measurement_type TO cube_user_group;


--
-- TOC entry 4661 (class 0 OID 0)
-- Dependencies: 252
-- Name: observation; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE observation FROM PUBLIC;
REVOKE ALL ON TABLE observation FROM cube_admin;
GRANT ALL ON TABLE observation TO cube_admin;
GRANT ALL ON TABLE observation TO cube_admin_group;
GRANT SELECT ON TABLE observation TO cube_user_group;


--
-- TOC entry 4663 (class 0 OID 0)
-- Dependencies: 253
-- Name: observation_type; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE observation_type FROM PUBLIC;
REVOKE ALL ON TABLE observation_type FROM cube_admin;
GRANT ALL ON TABLE observation_type TO cube_admin;
GRANT ALL ON TABLE observation_type TO cube_admin_group;
GRANT SELECT ON TABLE observation_type TO cube_user_group;


--
-- TOC entry 4665 (class 0 OID 0)
-- Dependencies: 254
-- Name: platform; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE platform FROM PUBLIC;
REVOKE ALL ON TABLE platform FROM cube_admin;
GRANT ALL ON TABLE platform TO cube_admin;
GRANT ALL ON TABLE platform TO cube_admin_group;
GRANT SELECT ON TABLE platform TO cube_user_group;


--
-- TOC entry 4667 (class 0 OID 0)
-- Dependencies: 255
-- Name: platform_type; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE platform_type FROM PUBLIC;
REVOKE ALL ON TABLE platform_type FROM cube_admin;
GRANT ALL ON TABLE platform_type TO cube_admin;
GRANT ALL ON TABLE platform_type TO cube_admin_group;
GRANT SELECT ON TABLE platform_type TO cube_user_group;


--
-- TOC entry 4670 (class 0 OID 0)
-- Dependencies: 247
-- Name: storage; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE storage FROM PUBLIC;
REVOKE ALL ON TABLE storage FROM cube_admin;
GRANT ALL ON TABLE storage TO cube_admin;
GRANT ALL ON TABLE storage TO cube_admin_group;
GRANT SELECT ON TABLE storage TO cube_user_group;


--
-- TOC entry 4672 (class 0 OID 0)
-- Dependencies: 248
-- Name: storage_dataset; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE storage_dataset FROM PUBLIC;
REVOKE ALL ON TABLE storage_dataset FROM cube_admin;
GRANT ALL ON TABLE storage_dataset TO cube_admin;
GRANT ALL ON TABLE storage_dataset TO cube_admin_group;
GRANT SELECT ON TABLE storage_dataset TO cube_user_group;


--
-- TOC entry 4674 (class 0 OID 0)
-- Dependencies: 249
-- Name: storage_dimension; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE storage_dimension FROM PUBLIC;
REVOKE ALL ON TABLE storage_dimension FROM cube_admin;
GRANT ALL ON TABLE storage_dimension TO cube_admin;
GRANT ALL ON TABLE storage_dimension TO cube_admin_group;
GRANT SELECT ON TABLE storage_dimension TO cube_user_group;


--
-- TOC entry 4676 (class 0 OID 0)
-- Dependencies: 251
-- Name: storage_type_measurement_type; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE storage_type_measurement_type FROM PUBLIC;
REVOKE ALL ON TABLE storage_type_measurement_type FROM cube_admin;
GRANT ALL ON TABLE storage_type_measurement_type TO cube_admin;
GRANT ALL ON TABLE storage_type_measurement_type TO cube_admin_group;
GRANT SELECT ON TABLE storage_type_measurement_type TO cube_user_group;


-- Completed on 2015-05-28 16:28:52 AEST

--
-- PostgreSQL database dump complete
--

