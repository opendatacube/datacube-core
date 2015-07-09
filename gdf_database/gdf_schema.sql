--
-- PostgreSQL database dump
--

-- Dumped from database version 9.3.7
-- Dumped by pg_dump version 9.3.1
-- Started on 2015-07-09 13:23:09

SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;

--
-- TOC entry 6 (class 2615 OID 3506076)
-- Name: earth_observation; Type: SCHEMA; Schema: -; Owner: cube_admin
--

CREATE SCHEMA earth_observation;


ALTER SCHEMA earth_observation OWNER TO cube_admin;

--
-- TOC entry 7 (class 2615 OID 3506077)
-- Name: topology; Type: SCHEMA; Schema: -; Owner: cube_admin
--

CREATE SCHEMA topology;


ALTER SCHEMA topology OWNER TO cube_admin;

--
-- TOC entry 9 (class 2615 OID 3506078)
-- Name: ztmp; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA ztmp;


ALTER SCHEMA ztmp OWNER TO postgres;

--
-- TOC entry 3143 (class 0 OID 0)
-- Dependencies: 9
-- Name: SCHEMA ztmp; Type: COMMENT; Schema: -; Owner: postgres
--

COMMENT ON SCHEMA ztmp IS 'Temporary schema';


--
-- TOC entry 213 (class 3079 OID 12617)
-- Name: plpgsql; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;


--
-- TOC entry 3145 (class 0 OID 0)
-- Dependencies: 213
-- Name: EXTENSION plpgsql; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';


SET search_path = public, pg_catalog;

--
-- TOC entry 559 (class 1247 OID 3506081)
-- Name: attribute_value_type; Type: TYPE; Schema: public; Owner: cube_admin
--

CREATE TYPE attribute_value_type AS (
	attribute_type_id bigint,
	attribute_id bigint,
	attribute_value_id bigint
);


ALTER TYPE public.attribute_value_type OWNER TO cube_admin;

--
-- TOC entry 562 (class 1247 OID 3506084)
-- Name: category_id_level_type; Type: TYPE; Schema: public; Owner: cube_admin
--

CREATE TYPE category_id_level_type AS (
	category_type_id bigint,
	category_id bigint,
	level integer
);


ALTER TYPE public.category_id_level_type OWNER TO cube_admin;

--
-- TOC entry 565 (class 1247 OID 3506087)
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
-- TOC entry 176 (class 1259 OID 3506088)
-- Name: spectral_parameters; Type: TABLE; Schema: earth_observation; Owner: cube_admin; Tablespace: 
--

CREATE TABLE spectral_parameters (
    measurement_metatype_id bigint NOT NULL,
    measurement_type_id bigint NOT NULL
);


ALTER TABLE earth_observation.spectral_parameters OWNER TO cube_admin;

--
-- TOC entry 3146 (class 0 OID 0)
-- Dependencies: 176
-- Name: TABLE spectral_parameters; Type: COMMENT; Schema: earth_observation; Owner: cube_admin
--

COMMENT ON TABLE spectral_parameters IS 'Configuration: Spectral band parameters';


SET search_path = public, pg_catalog;

--
-- TOC entry 177 (class 1259 OID 3506091)
-- Name: dataset; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE dataset (
    dataset_type_id bigint NOT NULL,
    dataset_id bigint NOT NULL,
    observation_type_id bigint NOT NULL,
    observation_id bigint NOT NULL,
    dataset_location character varying(254),
    creation_datetime timestamp with time zone
);


ALTER TABLE public.dataset OWNER TO cube_admin;

--
-- TOC entry 3148 (class 0 OID 0)
-- Dependencies: 177
-- Name: TABLE dataset; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE dataset IS 'Data: Source dataset (file) ingested.
An example would be a dataset for a particular NBAR Landsat scene.';


--
-- TOC entry 178 (class 1259 OID 3506094)
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
-- TOC entry 3150 (class 0 OID 0)
-- Dependencies: 178
-- Name: TABLE dataset_dimension; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE dataset_dimension IS 'Data: Dimensional parameters for each source dataset.
Each dataset/dimension will have specific max/min/indexing values showing the range covered by the dataset in that particular dimension.';


--
-- TOC entry 211 (class 1259 OID 3506727)
-- Name: dataset_id_seq; Type: SEQUENCE; Schema: public; Owner: cube_admin
--

CREATE SEQUENCE dataset_id_seq
    START WITH 100
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.dataset_id_seq OWNER TO cube_admin;

--
-- TOC entry 179 (class 1259 OID 3506099)
-- Name: dataset_metadata; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE dataset_metadata (
    dataset_type_id bigint NOT NULL,
    dataset_id bigint NOT NULL,
    metadata_xml xml NOT NULL
);


ALTER TABLE public.dataset_metadata OWNER TO cube_admin;

--
-- TOC entry 3152 (class 0 OID 0)
-- Dependencies: 179
-- Name: TABLE dataset_metadata; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE dataset_metadata IS 'Data: Lookup table for dataset-level metadata (one:one)';


--
-- TOC entry 180 (class 1259 OID 3506105)
-- Name: dataset_type; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE dataset_type (
    dataset_type_id bigint NOT NULL,
    dataset_type_name character varying(254),
    dataset_type_tag character varying(32)
);


ALTER TABLE public.dataset_type OWNER TO cube_admin;

--
-- TOC entry 3154 (class 0 OID 0)
-- Dependencies: 180
-- Name: TABLE dataset_type; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE dataset_type IS 'Configuration: Type of source dataset (processing level)';


--
-- TOC entry 181 (class 1259 OID 3506108)
-- Name: dataset_type_dimension; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE dataset_type_dimension (
    dataset_type_id bigint NOT NULL,
    domain_id bigint NOT NULL,
    dimension_id bigint NOT NULL,
    dimension_order smallint NOT NULL
);


ALTER TABLE public.dataset_type_dimension OWNER TO cube_admin;

--
-- TOC entry 3156 (class 0 OID 0)
-- Dependencies: 181
-- Name: TABLE dataset_type_dimension; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE dataset_type_dimension IS 'Configuration: Association between dataset type and dimensions. Used to define dimensionality of source dataset types';


--
-- TOC entry 182 (class 1259 OID 3506111)
-- Name: dataset_type_domain; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE dataset_type_domain (
    dataset_type_id bigint NOT NULL,
    domain_id bigint NOT NULL,
    reference_system_id bigint
);


ALTER TABLE public.dataset_type_domain OWNER TO cube_admin;

--
-- TOC entry 3158 (class 0 OID 0)
-- Dependencies: 182
-- Name: TABLE dataset_type_domain; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE dataset_type_domain IS 'Configuration: Association between dataset types and domains (many-many).
Used to define which domains cover a given dataset type';


--
-- TOC entry 183 (class 1259 OID 3506114)
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
-- TOC entry 3160 (class 0 OID 0)
-- Dependencies: 183
-- Name: TABLE dataset_type_measurement_type; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE dataset_type_measurement_type IS 'Configuration: Associations between dataset types and measurement types (one-many)
e.g. associations between Landsat 7 NBAR and specific surface-reflectance corrected Landsat 7 bands';


--
-- TOC entry 184 (class 1259 OID 3506117)
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
-- TOC entry 3162 (class 0 OID 0)
-- Dependencies: 184
-- Name: TABLE datatype; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE datatype IS 'Configuration: Lookup table for measurement_type datatypes.';


--
-- TOC entry 185 (class 1259 OID 3506120)
-- Name: dimension; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE dimension (
    dimension_id bigint NOT NULL,
    dimension_name character varying(50) NOT NULL,
    dimension_tag character varying(8) NOT NULL
);


ALTER TABLE public.dimension OWNER TO cube_admin;

--
-- TOC entry 3164 (class 0 OID 0)
-- Dependencies: 185
-- Name: TABLE dimension; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE dimension IS 'Configuration: Dimensions for n-dimensional data structures, e.g. x,y,z,t';


--
-- TOC entry 186 (class 1259 OID 3506123)
-- Name: dimension_domain; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE dimension_domain (
    domain_id bigint NOT NULL,
    dimension_id bigint NOT NULL
);


ALTER TABLE public.dimension_domain OWNER TO cube_admin;

--
-- TOC entry 3166 (class 0 OID 0)
-- Dependencies: 186
-- Name: TABLE dimension_domain; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE dimension_domain IS 'Configuration: Many-many  mapping between dimensions and domains to allow multiple dimensions to be included in multiple domains.
For example, the z dimension could be managed in a Z-spatial domain, or in an XYZ-spatial domain.';


--
-- TOC entry 187 (class 1259 OID 3506126)
-- Name: domain; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE domain (
    domain_id bigint NOT NULL,
    domain_name character varying(16),
    domain_tag character varying(16)
);


ALTER TABLE public.domain OWNER TO cube_admin;

--
-- TOC entry 3168 (class 0 OID 0)
-- Dependencies: 187
-- Name: TABLE domain; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE domain IS 'Configuration: Domain groupings of dimensions (e.g. spectral, spatial XY, spatial XYZ, temporal)';


--
-- TOC entry 188 (class 1259 OID 3506129)
-- Name: indexing_type; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE indexing_type (
    indexing_type_id smallint NOT NULL,
    indexing_type_name character varying(128)
);


ALTER TABLE public.indexing_type OWNER TO cube_admin;

--
-- TOC entry 3170 (class 0 OID 0)
-- Dependencies: 188
-- Name: TABLE indexing_type; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE indexing_type IS 'Configuration: Lookup table to manage what kind of indexing to apply to a given dimension';


--
-- TOC entry 189 (class 1259 OID 3506132)
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
-- TOC entry 3172 (class 0 OID 0)
-- Dependencies: 189
-- Name: TABLE reference_system; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE reference_system IS 'Configuration: Coordinate reference systems for aplication to specific domains.
e.g. EPSG:4326, seconds since 1/1/1970 0:00, etc.';


--
-- TOC entry 190 (class 1259 OID 3506135)
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
-- TOC entry 3174 (class 0 OID 0)
-- Dependencies: 190
-- Name: TABLE reference_system_indexing; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE reference_system_indexing IS 'Configuration: Optional non-linear indexing for dimension in a given domain.
e.g. A spectral dimension containing multple bands needs to be indexed by band number, and each band number can be associated with a given measurement_type.';


--
-- TOC entry 191 (class 1259 OID 3506138)
-- Name: storage_type; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE storage_type (
    storage_type_id bigint NOT NULL,
    storage_type_name character varying(254),
    storage_type_tag character varying(16),
    storage_type_location character varying(256) NOT NULL
);


ALTER TABLE public.storage_type OWNER TO cube_admin;

--
-- TOC entry 3176 (class 0 OID 0)
-- Dependencies: 191
-- Name: TABLE storage_type; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE storage_type IS 'Configuration: storage parameter lookup table. Used TO manage different storage_types';


--
-- TOC entry 3177 (class 0 OID 0)
-- Dependencies: 191
-- Name: COLUMN storage_type.storage_type_location; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN storage_type.storage_type_location IS 'Root directory for this storage type';


--
-- TOC entry 192 (class 1259 OID 3506144)
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
    index_reference_system_id bigint,
    reverse_index boolean DEFAULT false NOT NULL
);


ALTER TABLE public.storage_type_dimension OWNER TO cube_admin;

--
-- TOC entry 3179 (class 0 OID 0)
-- Dependencies: 192
-- Name: TABLE storage_type_dimension; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE storage_type_dimension IS 'Configuration: Association between storage type and dimensions. Used TO define dimensionality of storage type';


--
-- TOC entry 3180 (class 0 OID 0)
-- Dependencies: 192
-- Name: COLUMN storage_type_dimension.reverse_index; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN storage_type_dimension.reverse_index IS 'Flag indicating whether sense of indexing values should be the reverse of the array indices (e.g. Latitude with spatial origin in UL corner)';


--
-- TOC entry 193 (class 1259 OID 3506147)
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
-- TOC entry 194 (class 1259 OID 3506155)
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
-- TOC entry 195 (class 1259 OID 3506159)
-- Name: property; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE property (
    property_id bigint NOT NULL,
    property_name character varying(32),
    datatype_id smallint
);


ALTER TABLE public.property OWNER TO cube_admin;

--
-- TOC entry 196 (class 1259 OID 3506162)
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
-- TOC entry 3185 (class 0 OID 0)
-- Dependencies: 196
-- Name: TABLE storage_type_dimension_property; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE storage_type_dimension_property IS 'Configuration: Metadata properties of dimension in storage type';


--
-- TOC entry 197 (class 1259 OID 3506165)
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
-- TOC entry 198 (class 1259 OID 3506170)
-- Name: instrument; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE instrument (
    instrument_type_id bigint NOT NULL,
    instrument_id bigint NOT NULL,
    instrument_name character varying(128),
    platform_type_id bigint,
    platform_id bigint,
    instrument_tag character varying(32)
);


ALTER TABLE public.instrument OWNER TO cube_admin;

--
-- TOC entry 3188 (class 0 OID 0)
-- Dependencies: 198
-- Name: TABLE instrument; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE instrument IS 'Configuration: Instrument used to gather observations.
An example would be the ETM+ sensor on the Landsat 7 platform';


--
-- TOC entry 199 (class 1259 OID 3506173)
-- Name: instrument_type; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE instrument_type (
    instrument_type_id bigint NOT NULL,
    instrument_type_name character varying(128)
);


ALTER TABLE public.instrument_type OWNER TO cube_admin;

--
-- TOC entry 3190 (class 0 OID 0)
-- Dependencies: 199
-- Name: TABLE instrument_type; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE instrument_type IS 'Configuration: Lookup table for instrument category';


--
-- TOC entry 200 (class 1259 OID 3506176)
-- Name: measurement_metatype; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE measurement_metatype (
    measurement_metatype_id bigint NOT NULL,
    measurement_metatype_name character varying(254)
);


ALTER TABLE public.measurement_metatype OWNER TO cube_admin;

--
-- TOC entry 3192 (class 0 OID 0)
-- Dependencies: 200
-- Name: TABLE measurement_metatype; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE measurement_metatype IS 'Configuration: General type of measurement (e.g. spectral band)';


--
-- TOC entry 201 (class 1259 OID 3506179)
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
-- TOC entry 3194 (class 0 OID 0)
-- Dependencies: 201
-- Name: TABLE measurement_type; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE measurement_type IS 'Configuration: Description of measurement(s) held in n-dimensional data structures: e.g. bands';


--
-- TOC entry 202 (class 1259 OID 3506182)
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
-- TOC entry 3196 (class 0 OID 0)
-- Dependencies: 202
-- Name: TABLE observation; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE observation IS 'Data: Source observation for datasets.
Analagous to old "acquisition" table in AGDC version 0 DB';


--
-- TOC entry 212 (class 1259 OID 3506729)
-- Name: observation_id_seq; Type: SEQUENCE; Schema: public; Owner: cube_admin
--

CREATE SEQUENCE observation_id_seq
    START WITH 100
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.observation_id_seq OWNER TO cube_admin;

--
-- TOC entry 203 (class 1259 OID 3506187)
-- Name: observation_type; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE observation_type (
    observation_type_id bigint NOT NULL,
    observation_type_name character varying(254)
);


ALTER TABLE public.observation_type OWNER TO cube_admin;

--
-- TOC entry 3198 (class 0 OID 0)
-- Dependencies: 203
-- Name: TABLE observation_type; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE observation_type IS 'Configuration: Type of source observation';


--
-- TOC entry 204 (class 1259 OID 3506190)
-- Name: platform; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE platform (
    platform_type_id bigint NOT NULL,
    platform_id bigint NOT NULL,
    platform_name character varying(128)
);


ALTER TABLE public.platform OWNER TO cube_admin;

--
-- TOC entry 3200 (class 0 OID 0)
-- Dependencies: 204
-- Name: TABLE platform; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE platform IS 'Configuration: Platform on which instrument is mounted.
An example would be a specific satellite such as Landsat 7';


--
-- TOC entry 205 (class 1259 OID 3506193)
-- Name: platform_type; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE platform_type (
    platform_type_id bigint NOT NULL,
    platform_type_name character varying(128)
);


ALTER TABLE public.platform_type OWNER TO cube_admin;

--
-- TOC entry 3202 (class 0 OID 0)
-- Dependencies: 205
-- Name: TABLE platform_type; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE platform_type IS 'Configuration: Lookup table for platform category
e.g. Satellite or Ship';


--
-- TOC entry 206 (class 1259 OID 3506196)
-- Name: storage; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE storage (
    storage_type_id bigint NOT NULL,
    storage_id bigint NOT NULL,
    storage_version integer NOT NULL,
    storage_location character varying(354),
    md5_checksum character(32),
    storage_bytes bigint,
    spatial_footprint_id bigint,
    max_index bigint
);


ALTER TABLE public.storage OWNER TO cube_admin;

--
-- TOC entry 3204 (class 0 OID 0)
-- Dependencies: 206
-- Name: TABLE storage; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE storage IS 'Data: n-dimensional data structure instances';


--
-- TOC entry 207 (class 1259 OID 3506199)
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
-- TOC entry 3206 (class 0 OID 0)
-- Dependencies: 207
-- Name: TABLE storage_dataset; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE storage_dataset IS 'Data: Association between storage and dataset instances (many-many)';


--
-- TOC entry 208 (class 1259 OID 3506202)
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
-- TOC entry 3208 (class 0 OID 0)
-- Dependencies: 208
-- Name: TABLE storage_dimension; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE storage_dimension IS 'Data: Association between storage and dimensions. Used to define attributes for each dimension in storage instances';


--
-- TOC entry 210 (class 1259 OID 3506725)
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
-- TOC entry 209 (class 1259 OID 3506207)
-- Name: storage_type_measurement_type; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE storage_type_measurement_type (
    storage_type_id bigint NOT NULL,
    measurement_metatype_id bigint NOT NULL,
    measurement_type_id bigint NOT NULL,
    datatype_id smallint,
    measurement_type_index smallint,
    nodata_value double precision
);


ALTER TABLE public.storage_type_measurement_type OWNER TO cube_admin;

--
-- TOC entry 3210 (class 0 OID 0)
-- Dependencies: 209
-- Name: TABLE storage_type_measurement_type; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE storage_type_measurement_type IS 'Configuration: Associations between n-dimensional data structure types and measurement types (i.e. variables) (many-many)';


SET search_path = earth_observation, pg_catalog;

--
-- TOC entry 2846 (class 2606 OID 3506211)
-- Name: pk_spectral_parameters; Type: CONSTRAINT; Schema: earth_observation; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY spectral_parameters
    ADD CONSTRAINT pk_spectral_parameters PRIMARY KEY (measurement_metatype_id, measurement_type_id);


SET search_path = public, pg_catalog;

--
-- TOC entry 2850 (class 2606 OID 3506213)
-- Name: pk_dataset; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dataset
    ADD CONSTRAINT pk_dataset PRIMARY KEY (dataset_type_id, dataset_id);


--
-- TOC entry 2854 (class 2606 OID 3506215)
-- Name: pk_dataset_dimension; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dataset_dimension
    ADD CONSTRAINT pk_dataset_dimension PRIMARY KEY (dataset_type_id, dataset_id, domain_id, dimension_id);


--
-- TOC entry 2857 (class 2606 OID 3506217)
-- Name: pk_dataset_metadata; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dataset_metadata
    ADD CONSTRAINT pk_dataset_metadata PRIMARY KEY (dataset_type_id, dataset_id);


--
-- TOC entry 2859 (class 2606 OID 3506219)
-- Name: pk_dataset_type; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dataset_type
    ADD CONSTRAINT pk_dataset_type PRIMARY KEY (dataset_type_id);


--
-- TOC entry 2865 (class 2606 OID 3506221)
-- Name: pk_dataset_type_dimension; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dataset_type_dimension
    ADD CONSTRAINT pk_dataset_type_dimension PRIMARY KEY (dataset_type_id, domain_id, dimension_id);


--
-- TOC entry 2872 (class 2606 OID 3506223)
-- Name: pk_dataset_type_domain; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dataset_type_domain
    ADD CONSTRAINT pk_dataset_type_domain PRIMARY KEY (dataset_type_id, domain_id);


--
-- TOC entry 2877 (class 2606 OID 3506225)
-- Name: pk_dataset_type_measurement_type; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dataset_type_measurement_type
    ADD CONSTRAINT pk_dataset_type_measurement_type PRIMARY KEY (dataset_type_id, measurement_metatype_id, measurement_type_id);


--
-- TOC entry 2881 (class 2606 OID 3506227)
-- Name: pk_datatype; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY datatype
    ADD CONSTRAINT pk_datatype PRIMARY KEY (datatype_id);


--
-- TOC entry 2885 (class 2606 OID 3506229)
-- Name: pk_dimension; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dimension
    ADD CONSTRAINT pk_dimension PRIMARY KEY (dimension_id);


--
-- TOC entry 2893 (class 2606 OID 3506231)
-- Name: pk_dimension_domain; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dimension_domain
    ADD CONSTRAINT pk_dimension_domain PRIMARY KEY (domain_id, dimension_id);


--
-- TOC entry 2895 (class 2606 OID 3506233)
-- Name: pk_domain; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY domain
    ADD CONSTRAINT pk_domain PRIMARY KEY (domain_id);


--
-- TOC entry 2899 (class 2606 OID 3506235)
-- Name: pk_indexing_type; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY indexing_type
    ADD CONSTRAINT pk_indexing_type PRIMARY KEY (indexing_type_id);


--
-- TOC entry 2932 (class 2606 OID 3506237)
-- Name: pk_instrument; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY instrument
    ADD CONSTRAINT pk_instrument PRIMARY KEY (instrument_type_id, instrument_id);


--
-- TOC entry 2936 (class 2606 OID 3506239)
-- Name: pk_instrument_type; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY instrument_type
    ADD CONSTRAINT pk_instrument_type PRIMARY KEY (instrument_type_id);


--
-- TOC entry 2940 (class 2606 OID 3506241)
-- Name: pk_measurement_metatype; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY measurement_metatype
    ADD CONSTRAINT pk_measurement_metatype PRIMARY KEY (measurement_metatype_id);


--
-- TOC entry 2945 (class 2606 OID 3506243)
-- Name: pk_measurement_type; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY measurement_type
    ADD CONSTRAINT pk_measurement_type PRIMARY KEY (measurement_metatype_id, measurement_type_id);


--
-- TOC entry 2949 (class 2606 OID 3506245)
-- Name: pk_observation; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY observation
    ADD CONSTRAINT pk_observation PRIMARY KEY (observation_type_id, observation_id);


--
-- TOC entry 2951 (class 2606 OID 3506247)
-- Name: pk_observation_type; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY observation_type
    ADD CONSTRAINT pk_observation_type PRIMARY KEY (observation_type_id);


--
-- TOC entry 2956 (class 2606 OID 3506249)
-- Name: pk_platform; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY platform
    ADD CONSTRAINT pk_platform PRIMARY KEY (platform_type_id, platform_id);


--
-- TOC entry 2960 (class 2606 OID 3506251)
-- Name: pk_platform_type; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY platform_type
    ADD CONSTRAINT pk_platform_type PRIMARY KEY (platform_type_id);


--
-- TOC entry 2903 (class 2606 OID 3506253)
-- Name: pk_reference_system; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY reference_system
    ADD CONSTRAINT pk_reference_system PRIMARY KEY (reference_system_id);


--
-- TOC entry 2909 (class 2606 OID 3506255)
-- Name: pk_reference_system_indexing; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY reference_system_indexing
    ADD CONSTRAINT pk_reference_system_indexing PRIMARY KEY (reference_system_id, array_index);


--
-- TOC entry 2967 (class 2606 OID 3506257)
-- Name: pk_storage; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY storage
    ADD CONSTRAINT pk_storage PRIMARY KEY (storage_type_id, storage_id, storage_version);


--
-- TOC entry 2973 (class 2606 OID 3506259)
-- Name: pk_storage_dataset; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY storage_dataset
    ADD CONSTRAINT pk_storage_dataset PRIMARY KEY (storage_type_id, storage_id, storage_version, dataset_type_id, dataset_id);


--
-- TOC entry 2980 (class 2606 OID 3506261)
-- Name: pk_storage_dimension; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY storage_dimension
    ADD CONSTRAINT pk_storage_dimension PRIMARY KEY (storage_type_id, storage_id, storage_version, domain_id, dimension_id);


--
-- TOC entry 2985 (class 2606 OID 3506263)
-- Name: pk_storage_measurement_type; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY storage_type_measurement_type
    ADD CONSTRAINT pk_storage_measurement_type PRIMARY KEY (storage_type_id, measurement_metatype_id, measurement_type_id);


--
-- TOC entry 2911 (class 2606 OID 3506265)
-- Name: pk_storage_type; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY storage_type
    ADD CONSTRAINT pk_storage_type PRIMARY KEY (storage_type_id);


--
-- TOC entry 2918 (class 2606 OID 3506267)
-- Name: pk_storage_type_dimension; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY storage_type_dimension
    ADD CONSTRAINT pk_storage_type_dimension PRIMARY KEY (storage_type_id, domain_id, dimension_id);


--
-- TOC entry 2928 (class 2606 OID 3506269)
-- Name: pk_storage_type_dimension_property; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY storage_type_dimension_property
    ADD CONSTRAINT pk_storage_type_dimension_property PRIMARY KEY (storage_type_id, domain_id, dimension_id, property_id);


--
-- TOC entry 2922 (class 2606 OID 3506271)
-- Name: property_pkey; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY property
    ADD CONSTRAINT property_pkey PRIMARY KEY (property_id);


--
-- TOC entry 2861 (class 2606 OID 3506273)
-- Name: uq_dataset_type_dataset_type_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dataset_type
    ADD CONSTRAINT uq_dataset_type_dataset_type_name UNIQUE (dataset_type_name);


--
-- TOC entry 2867 (class 2606 OID 3506688)
-- Name: uq_dataset_type_dimension_dataset_type_id_dimension_order; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dataset_type_dimension
    ADD CONSTRAINT uq_dataset_type_dimension_dataset_type_id_dimension_order UNIQUE (dataset_type_id, dimension_order);


--
-- TOC entry 2879 (class 2606 OID 3506275)
-- Name: uq_dataset_type_measurement_type_dataset_type; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dataset_type_measurement_type
    ADD CONSTRAINT uq_dataset_type_measurement_type_dataset_type UNIQUE (dataset_type_id, measurement_type_index);


--
-- TOC entry 2883 (class 2606 OID 3506277)
-- Name: uq_datatype_datatype_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY datatype
    ADD CONSTRAINT uq_datatype_datatype_name UNIQUE (datatype_name);


--
-- TOC entry 2887 (class 2606 OID 3506279)
-- Name: uq_dimension_dimension_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dimension
    ADD CONSTRAINT uq_dimension_dimension_name UNIQUE (dimension_name);


--
-- TOC entry 2889 (class 2606 OID 3506281)
-- Name: uq_dimension_dimension_tag; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dimension
    ADD CONSTRAINT uq_dimension_dimension_tag UNIQUE (dimension_tag);


--
-- TOC entry 2897 (class 2606 OID 3506283)
-- Name: uq_domain_domain_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY domain
    ADD CONSTRAINT uq_domain_domain_name UNIQUE (domain_name);


--
-- TOC entry 2901 (class 2606 OID 3506285)
-- Name: uq_indexing_type_indexing_type_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY indexing_type
    ADD CONSTRAINT uq_indexing_type_indexing_type_name UNIQUE (indexing_type_name);


--
-- TOC entry 2934 (class 2606 OID 3506287)
-- Name: uq_instrument_instrument_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY instrument
    ADD CONSTRAINT uq_instrument_instrument_name UNIQUE (instrument_name);


--
-- TOC entry 2938 (class 2606 OID 3506289)
-- Name: uq_instrument_type_instrument_type_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY instrument_type
    ADD CONSTRAINT uq_instrument_type_instrument_type_name UNIQUE (instrument_type_name);


--
-- TOC entry 2942 (class 2606 OID 3506291)
-- Name: uq_measurement_metatype_measurement_metatype_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY measurement_metatype
    ADD CONSTRAINT uq_measurement_metatype_measurement_metatype_name UNIQUE (measurement_metatype_name);


--
-- TOC entry 2953 (class 2606 OID 3506293)
-- Name: uq_observation_type_observation_type_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY observation_type
    ADD CONSTRAINT uq_observation_type_observation_type_name UNIQUE (observation_type_name);


--
-- TOC entry 2958 (class 2606 OID 3506295)
-- Name: uq_platform_platform_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY platform
    ADD CONSTRAINT uq_platform_platform_name UNIQUE (platform_name);


--
-- TOC entry 2962 (class 2606 OID 3506297)
-- Name: uq_platform_type_platform_type_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY platform_type
    ADD CONSTRAINT uq_platform_type_platform_type_name UNIQUE (platform_type_name);


--
-- TOC entry 2924 (class 2606 OID 3506299)
-- Name: uq_property_property_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY property
    ADD CONSTRAINT uq_property_property_name UNIQUE (property_name);


--
-- TOC entry 2905 (class 2606 OID 3506301)
-- Name: uq_reference_system_reference_system_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY reference_system
    ADD CONSTRAINT uq_reference_system_reference_system_name UNIQUE (reference_system_name);


--
-- TOC entry 2969 (class 2606 OID 3506303)
-- Name: uq_storage_storage_location; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY storage
    ADD CONSTRAINT uq_storage_storage_location UNIQUE (storage_location);


--
-- TOC entry 2920 (class 2606 OID 3506305)
-- Name: uq_storage_type_dimension_storage_type_dimension; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY storage_type_dimension
    ADD CONSTRAINT uq_storage_type_dimension_storage_type_dimension UNIQUE (storage_type_id, dimension_id);


--
-- TOC entry 3212 (class 0 OID 0)
-- Dependencies: 2920
-- Name: CONSTRAINT uq_storage_type_dimension_storage_type_dimension ON storage_type_dimension; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON CONSTRAINT uq_storage_type_dimension_storage_type_dimension ON storage_type_dimension IS 'Unique constraint to ensure each dimension is only represented once in each storage_type';


--
-- TOC entry 2987 (class 2606 OID 3506307)
-- Name: uq_storage_type_measurement_type_storage_type_id_measurement_ty; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY storage_type_measurement_type
    ADD CONSTRAINT uq_storage_type_measurement_type_storage_type_id_measurement_ty UNIQUE (storage_type_id, measurement_type_index);


--
-- TOC entry 2913 (class 2606 OID 3506309)
-- Name: uq_storage_type_storage_type_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY storage_type
    ADD CONSTRAINT uq_storage_type_storage_type_name UNIQUE (storage_type_name);


SET search_path = earth_observation, pg_catalog;

--
-- TOC entry 2844 (class 1259 OID 3506310)
-- Name: fki_spectral_parameters_measurement_type; Type: INDEX; Schema: earth_observation; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_spectral_parameters_measurement_type ON spectral_parameters USING btree (measurement_metatype_id, measurement_type_id);


SET search_path = public, pg_catalog;

--
-- TOC entry 2847 (class 1259 OID 3506311)
-- Name: fki_dataset_dataset_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_dataset_type ON dataset USING btree (dataset_type_id);


--
-- TOC entry 2851 (class 1259 OID 3506312)
-- Name: fki_dataset_dimension_dataset; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_dimension_dataset ON dataset_dimension USING btree (dataset_type_id, dataset_id);


--
-- TOC entry 2852 (class 1259 OID 3506313)
-- Name: fki_dataset_dimension_dataset_type_dimension; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_dimension_dataset_type_dimension ON dataset_dimension USING btree (dataset_type_id, domain_id, dimension_id);


--
-- TOC entry 2855 (class 1259 OID 3506314)
-- Name: fki_dataset_metadata_dataset; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_metadata_dataset ON dataset_metadata USING btree (dataset_type_id, dataset_id);


--
-- TOC entry 2848 (class 1259 OID 3506315)
-- Name: fki_dataset_observation; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_observation ON dataset USING btree (observation_type_id, observation_id);


--
-- TOC entry 2862 (class 1259 OID 3506316)
-- Name: fki_dataset_type_dimension_dataset_type_domain; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_type_dimension_dataset_type_domain ON dataset_type_dimension USING btree (dataset_type_id, domain_id);


--
-- TOC entry 2863 (class 1259 OID 3506317)
-- Name: fki_dataset_type_dimension_dimension_domain; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_type_dimension_dimension_domain ON dataset_type_dimension USING btree (domain_id, dimension_id);


--
-- TOC entry 2868 (class 1259 OID 3506318)
-- Name: fki_dataset_type_domain_dataset_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_type_domain_dataset_type ON dataset_type_domain USING btree (dataset_type_id);


--
-- TOC entry 2869 (class 1259 OID 3506319)
-- Name: fki_dataset_type_domain_domain; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_type_domain_domain ON dataset_type_domain USING btree (domain_id);


--
-- TOC entry 2870 (class 1259 OID 3506320)
-- Name: fki_dataset_type_domain_reference_system; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_type_domain_reference_system ON dataset_type_domain USING btree (reference_system_id);


--
-- TOC entry 2873 (class 1259 OID 3506321)
-- Name: fki_dataset_type_measurement_metatype_datatype; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_type_measurement_metatype_datatype ON dataset_type_measurement_type USING btree (datatype_id);


--
-- TOC entry 2874 (class 1259 OID 3506322)
-- Name: fki_dataset_type_measurement_type_dataset_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_type_measurement_type_dataset_type ON dataset_type_measurement_type USING btree (dataset_type_id);


--
-- TOC entry 2875 (class 1259 OID 3506323)
-- Name: fki_dataset_type_measurement_type_measurement_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_type_measurement_type_measurement_type ON dataset_type_measurement_type USING btree (measurement_metatype_id, measurement_type_id);


--
-- TOC entry 2890 (class 1259 OID 3506324)
-- Name: fki_dimension_domain_dimension; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dimension_domain_dimension ON dimension_domain USING btree (dimension_id);


--
-- TOC entry 2891 (class 1259 OID 3506325)
-- Name: fki_dimension_domain_domain; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dimension_domain_domain ON dimension_domain USING btree (domain_id);


--
-- TOC entry 2929 (class 1259 OID 3506326)
-- Name: fki_instrument_instrument_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_instrument_instrument_type ON instrument USING btree (instrument_type_id);


--
-- TOC entry 2930 (class 1259 OID 3506327)
-- Name: fki_instrument_platform; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_instrument_platform ON instrument USING btree (platform_type_id, platform_id);


--
-- TOC entry 2943 (class 1259 OID 3506328)
-- Name: fki_measurement_type_measurement_metatype; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_measurement_type_measurement_metatype ON measurement_type USING btree (measurement_metatype_id);


--
-- TOC entry 2963 (class 1259 OID 3506329)
-- Name: fki_ndarray_footprint_ndarray_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_ndarray_footprint_ndarray_type ON storage USING btree (storage_type_id);


--
-- TOC entry 2946 (class 1259 OID 3506330)
-- Name: fki_observation_instrument; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_observation_instrument ON observation USING btree (instrument_type_id, instrument_id);


--
-- TOC entry 2947 (class 1259 OID 3506331)
-- Name: fki_observation_observation_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_observation_observation_type ON observation USING btree (observation_type_id);


--
-- TOC entry 2954 (class 1259 OID 3506332)
-- Name: fki_platform_platform_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_platform_platform_type ON platform USING btree (platform_type_id);


--
-- TOC entry 2906 (class 1259 OID 3506333)
-- Name: fki_reference_system_indexing_measurement_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_reference_system_indexing_measurement_type ON reference_system_indexing USING btree (measurement_metatype_id, measurement_type_id);


--
-- TOC entry 2907 (class 1259 OID 3506334)
-- Name: fki_reference_system_indexing_reference_system; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_reference_system_indexing_reference_system ON reference_system_indexing USING btree (reference_system_id);


--
-- TOC entry 2970 (class 1259 OID 3506335)
-- Name: fki_storage_dataset_dataset; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_storage_dataset_dataset ON storage_dataset USING btree (dataset_type_id, dataset_id);


--
-- TOC entry 2971 (class 1259 OID 3506336)
-- Name: fki_storage_dataset_storage; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_storage_dataset_storage ON storage_dataset USING btree (storage_type_id, storage_id, storage_version);


--
-- TOC entry 2974 (class 1259 OID 3506337)
-- Name: fki_storage_dimension_storage; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_storage_dimension_storage ON storage_dimension USING btree (storage_type_id, storage_id, storage_version);


--
-- TOC entry 2975 (class 1259 OID 3506338)
-- Name: fki_storage_dimension_storage_type_dimension; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_storage_dimension_storage_type_dimension ON storage_dimension USING btree (storage_type_id, domain_id, dimension_id);


--
-- TOC entry 2964 (class 1259 OID 3506339)
-- Name: fki_storage_spatial_footprint; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_storage_spatial_footprint ON storage USING btree (spatial_footprint_id);


--
-- TOC entry 2965 (class 1259 OID 3506340)
-- Name: fki_storage_storage_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_storage_storage_type ON storage USING btree (storage_type_id, storage_type_id);


--
-- TOC entry 2925 (class 1259 OID 3506341)
-- Name: fki_storage_type_dimension_attribute_property; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_storage_type_dimension_attribute_property ON storage_type_dimension_property USING btree (property_id);


--
-- TOC entry 2926 (class 1259 OID 3506342)
-- Name: fki_storage_type_dimension_attribute_storage_type_dimension; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_storage_type_dimension_attribute_storage_type_dimension ON storage_type_dimension_property USING btree (storage_type_id, domain_id, dimension_id);


--
-- TOC entry 2914 (class 1259 OID 3506343)
-- Name: fki_storage_type_dimension_dimension_domain; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_storage_type_dimension_dimension_domain ON storage_type_dimension USING btree (domain_id, dimension_id);


--
-- TOC entry 2915 (class 1259 OID 3506344)
-- Name: fki_storage_type_dimension_indexing_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_storage_type_dimension_indexing_type ON storage_type_dimension USING btree (indexing_type_id);


--
-- TOC entry 2916 (class 1259 OID 3506345)
-- Name: fki_storage_type_dimension_storage_type_id_fkey; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_storage_type_dimension_storage_type_id_fkey ON storage_type_dimension USING btree (storage_type_id, domain_id);


--
-- TOC entry 2981 (class 1259 OID 3506346)
-- Name: fki_storage_type_masurement_type_datatype; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_storage_type_masurement_type_datatype ON storage_type_measurement_type USING btree (datatype_id);


--
-- TOC entry 2982 (class 1259 OID 3506347)
-- Name: fki_storage_type_measurement_type_measurement_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_storage_type_measurement_type_measurement_type ON storage_type_measurement_type USING btree (measurement_metatype_id, measurement_type_id);


--
-- TOC entry 2983 (class 1259 OID 3506348)
-- Name: fki_storage_type_measurement_type_storage_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_storage_type_measurement_type_storage_type ON storage_type_measurement_type USING btree (storage_type_id);


--
-- TOC entry 2976 (class 1259 OID 3506349)
-- Name: idx_storage_dimension_storage_dimension_index; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX idx_storage_dimension_storage_dimension_index ON storage_dimension USING btree (storage_dimension_index);


--
-- TOC entry 2977 (class 1259 OID 3506350)
-- Name: idx_storage_dimension_storage_dimension_max; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX idx_storage_dimension_storage_dimension_max ON storage_dimension USING btree (storage_dimension_max);


--
-- TOC entry 2978 (class 1259 OID 3506351)
-- Name: idx_storage_dimension_storage_dimension_min; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX idx_storage_dimension_storage_dimension_min ON storage_dimension USING btree (storage_dimension_max);


SET search_path = earth_observation, pg_catalog;

--
-- TOC entry 2988 (class 2606 OID 3506352)
-- Name: fk_spectral_parameters_measurement_metatype; Type: FK CONSTRAINT; Schema: earth_observation; Owner: cube_admin
--

ALTER TABLE ONLY spectral_parameters
    ADD CONSTRAINT fk_spectral_parameters_measurement_metatype FOREIGN KEY (measurement_metatype_id, measurement_type_id) REFERENCES public.measurement_type(measurement_metatype_id, measurement_type_id);


SET search_path = public, pg_catalog;

--
-- TOC entry 2989 (class 2606 OID 3506357)
-- Name: fk_dataset_dataset_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset
    ADD CONSTRAINT fk_dataset_dataset_type FOREIGN KEY (dataset_type_id) REFERENCES dataset_type(dataset_type_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 2991 (class 2606 OID 3506362)
-- Name: fk_dataset_dimension_dataset; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset_dimension
    ADD CONSTRAINT fk_dataset_dimension_dataset FOREIGN KEY (dataset_type_id, dataset_id) REFERENCES dataset(dataset_type_id, dataset_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 2992 (class 2606 OID 3506367)
-- Name: fk_dataset_dimension_dataset_type_dimension; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset_dimension
    ADD CONSTRAINT fk_dataset_dimension_dataset_type_dimension FOREIGN KEY (dataset_type_id, domain_id, dimension_id) REFERENCES dataset_type_dimension(dataset_type_id, domain_id, dimension_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 2993 (class 2606 OID 3506372)
-- Name: fk_dataset_metadata_dataset; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset_metadata
    ADD CONSTRAINT fk_dataset_metadata_dataset FOREIGN KEY (dataset_type_id, dataset_id) REFERENCES dataset(dataset_type_id, dataset_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 2990 (class 2606 OID 3506377)
-- Name: fk_dataset_observation; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset
    ADD CONSTRAINT fk_dataset_observation FOREIGN KEY (observation_type_id, observation_id) REFERENCES observation(observation_type_id, observation_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 2994 (class 2606 OID 3506382)
-- Name: fk_dataset_type_dimension_dataset_type_domain; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset_type_dimension
    ADD CONSTRAINT fk_dataset_type_dimension_dataset_type_domain FOREIGN KEY (dataset_type_id, domain_id) REFERENCES dataset_type_domain(dataset_type_id, domain_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 2995 (class 2606 OID 3506387)
-- Name: fk_dataset_type_dimension_dimension_domain; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset_type_dimension
    ADD CONSTRAINT fk_dataset_type_dimension_dimension_domain FOREIGN KEY (domain_id, dimension_id) REFERENCES dimension_domain(domain_id, dimension_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 2996 (class 2606 OID 3506392)
-- Name: fk_dataset_type_domain_dataset_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset_type_domain
    ADD CONSTRAINT fk_dataset_type_domain_dataset_type FOREIGN KEY (dataset_type_id) REFERENCES dataset_type(dataset_type_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 2997 (class 2606 OID 3506397)
-- Name: fk_dataset_type_domain_domain; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset_type_domain
    ADD CONSTRAINT fk_dataset_type_domain_domain FOREIGN KEY (domain_id) REFERENCES domain(domain_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 2998 (class 2606 OID 3506402)
-- Name: fk_dataset_type_domain_reference_system; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset_type_domain
    ADD CONSTRAINT fk_dataset_type_domain_reference_system FOREIGN KEY (reference_system_id) REFERENCES reference_system(reference_system_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 2999 (class 2606 OID 3506407)
-- Name: fk_dataset_type_measurement_type_dataset_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset_type_measurement_type
    ADD CONSTRAINT fk_dataset_type_measurement_type_dataset_type FOREIGN KEY (dataset_type_id) REFERENCES dataset_type(dataset_type_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3000 (class 2606 OID 3506412)
-- Name: fk_dataset_type_measurement_type_datatype; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset_type_measurement_type
    ADD CONSTRAINT fk_dataset_type_measurement_type_datatype FOREIGN KEY (datatype_id) REFERENCES datatype(datatype_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3001 (class 2606 OID 3506417)
-- Name: fk_dataset_type_measurement_type_measurement_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset_type_measurement_type
    ADD CONSTRAINT fk_dataset_type_measurement_type_measurement_type FOREIGN KEY (measurement_metatype_id, measurement_type_id) REFERENCES measurement_type(measurement_metatype_id, measurement_type_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3002 (class 2606 OID 3506422)
-- Name: fk_dimension_domain_dimension; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dimension_domain
    ADD CONSTRAINT fk_dimension_domain_dimension FOREIGN KEY (dimension_id) REFERENCES dimension(dimension_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3003 (class 2606 OID 3506427)
-- Name: fk_dimension_domain_domain; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dimension_domain
    ADD CONSTRAINT fk_dimension_domain_domain FOREIGN KEY (domain_id) REFERENCES domain(domain_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3012 (class 2606 OID 3506432)
-- Name: fk_instrument_instrument_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY instrument
    ADD CONSTRAINT fk_instrument_instrument_type FOREIGN KEY (instrument_type_id) REFERENCES instrument_type(instrument_type_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3013 (class 2606 OID 3506437)
-- Name: fk_instrument_platform; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY instrument
    ADD CONSTRAINT fk_instrument_platform FOREIGN KEY (platform_type_id, platform_id) REFERENCES platform(platform_type_id, platform_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3014 (class 2606 OID 3506442)
-- Name: fk_measurement_type_measurement_metatype; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY measurement_type
    ADD CONSTRAINT fk_measurement_type_measurement_metatype FOREIGN KEY (measurement_metatype_id) REFERENCES measurement_metatype(measurement_metatype_id);


--
-- TOC entry 3015 (class 2606 OID 3506447)
-- Name: fk_observation_instrument; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY observation
    ADD CONSTRAINT fk_observation_instrument FOREIGN KEY (instrument_type_id, instrument_id) REFERENCES instrument(instrument_type_id, instrument_id);


--
-- TOC entry 3016 (class 2606 OID 3506452)
-- Name: fk_observation_observation_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY observation
    ADD CONSTRAINT fk_observation_observation_type FOREIGN KEY (observation_type_id) REFERENCES observation_type(observation_type_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3017 (class 2606 OID 3506457)
-- Name: fk_platform_platform_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY platform
    ADD CONSTRAINT fk_platform_platform_type FOREIGN KEY (platform_type_id) REFERENCES platform_type(platform_type_id);


--
-- TOC entry 3004 (class 2606 OID 3506462)
-- Name: fk_reference_system_indexing_measurement_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY reference_system_indexing
    ADD CONSTRAINT fk_reference_system_indexing_measurement_type FOREIGN KEY (measurement_metatype_id, measurement_type_id) REFERENCES measurement_type(measurement_metatype_id, measurement_type_id);


--
-- TOC entry 3005 (class 2606 OID 3506467)
-- Name: fk_reference_system_indexing_reference_system; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY reference_system_indexing
    ADD CONSTRAINT fk_reference_system_indexing_reference_system FOREIGN KEY (reference_system_id) REFERENCES reference_system(reference_system_id);


--
-- TOC entry 3019 (class 2606 OID 3506472)
-- Name: fk_storage_dataset_dataset; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY storage_dataset
    ADD CONSTRAINT fk_storage_dataset_dataset FOREIGN KEY (dataset_type_id, dataset_id) REFERENCES dataset(dataset_type_id, dataset_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3020 (class 2606 OID 3506477)
-- Name: fk_storage_dataset_storage; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY storage_dataset
    ADD CONSTRAINT fk_storage_dataset_storage FOREIGN KEY (storage_type_id, storage_id, storage_version) REFERENCES storage(storage_type_id, storage_id, storage_version) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3021 (class 2606 OID 3506482)
-- Name: fk_storage_dimension_storage; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY storage_dimension
    ADD CONSTRAINT fk_storage_dimension_storage FOREIGN KEY (storage_type_id, storage_id, storage_version) REFERENCES storage(storage_type_id, storage_id, storage_version) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3022 (class 2606 OID 3506487)
-- Name: fk_storage_dimension_storage_type_dimension; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY storage_dimension
    ADD CONSTRAINT fk_storage_dimension_storage_type_dimension FOREIGN KEY (storage_type_id, domain_id, dimension_id) REFERENCES storage_type_dimension(storage_type_id, domain_id, dimension_id);


--
-- TOC entry 3018 (class 2606 OID 3506492)
-- Name: fk_storage_storage_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY storage
    ADD CONSTRAINT fk_storage_storage_type FOREIGN KEY (storage_type_id) REFERENCES storage_type(storage_type_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3010 (class 2606 OID 3506497)
-- Name: fk_storage_type_dimension_attribute_storage_type_dimension; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY storage_type_dimension_property
    ADD CONSTRAINT fk_storage_type_dimension_attribute_storage_type_dimension FOREIGN KEY (storage_type_id, domain_id, dimension_id) REFERENCES storage_type_dimension(storage_type_id, domain_id, dimension_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3006 (class 2606 OID 3506502)
-- Name: fk_storage_type_dimension_dimension_domain; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY storage_type_dimension
    ADD CONSTRAINT fk_storage_type_dimension_dimension_domain FOREIGN KEY (domain_id, dimension_id) REFERENCES dimension_domain(domain_id, dimension_id) ON UPDATE CASCADE;


--
-- TOC entry 3007 (class 2606 OID 3506507)
-- Name: fk_storage_type_dimension_indexing_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY storage_type_dimension
    ADD CONSTRAINT fk_storage_type_dimension_indexing_type FOREIGN KEY (indexing_type_id) REFERENCES indexing_type(indexing_type_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3011 (class 2606 OID 3506512)
-- Name: fk_storage_type_dimension_property_property; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY storage_type_dimension_property
    ADD CONSTRAINT fk_storage_type_dimension_property_property FOREIGN KEY (property_id) REFERENCES property(property_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3008 (class 2606 OID 3506517)
-- Name: fk_storage_type_dimension_reference_system; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY storage_type_dimension
    ADD CONSTRAINT fk_storage_type_dimension_reference_system FOREIGN KEY (reference_system_id) REFERENCES reference_system(reference_system_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3009 (class 2606 OID 3506522)
-- Name: fk_storage_type_dimension_storage_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY storage_type_dimension
    ADD CONSTRAINT fk_storage_type_dimension_storage_type FOREIGN KEY (storage_type_id) REFERENCES storage_type(storage_type_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3023 (class 2606 OID 3506527)
-- Name: fk_storage_type_measurement_type_datatype; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY storage_type_measurement_type
    ADD CONSTRAINT fk_storage_type_measurement_type_datatype FOREIGN KEY (datatype_id) REFERENCES datatype(datatype_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3024 (class 2606 OID 3506532)
-- Name: fk_storage_type_measurement_type_measurement_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY storage_type_measurement_type
    ADD CONSTRAINT fk_storage_type_measurement_type_measurement_type FOREIGN KEY (measurement_metatype_id, measurement_type_id) REFERENCES measurement_type(measurement_metatype_id, measurement_type_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3025 (class 2606 OID 3506537)
-- Name: fk_storage_type_measurement_type_storage_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY storage_type_measurement_type
    ADD CONSTRAINT fk_storage_type_measurement_type_storage_type FOREIGN KEY (storage_type_id) REFERENCES storage_type(storage_type_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3142 (class 0 OID 0)
-- Dependencies: 8
-- Name: public; Type: ACL; Schema: -; Owner: postgres
--

REVOKE ALL ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM postgres;
GRANT ALL ON SCHEMA public TO postgres;
GRANT ALL ON SCHEMA public TO cube_admin;
GRANT ALL ON SCHEMA public TO PUBLIC;


--
-- TOC entry 3144 (class 0 OID 0)
-- Dependencies: 9
-- Name: ztmp; Type: ACL; Schema: -; Owner: postgres
--

REVOKE ALL ON SCHEMA ztmp FROM PUBLIC;
REVOKE ALL ON SCHEMA ztmp FROM postgres;
GRANT ALL ON SCHEMA ztmp TO postgres;
GRANT ALL ON SCHEMA ztmp TO PUBLIC;


SET search_path = earth_observation, pg_catalog;

--
-- TOC entry 3147 (class 0 OID 0)
-- Dependencies: 176
-- Name: spectral_parameters; Type: ACL; Schema: earth_observation; Owner: cube_admin
--

REVOKE ALL ON TABLE spectral_parameters FROM PUBLIC;
REVOKE ALL ON TABLE spectral_parameters FROM cube_admin;
GRANT ALL ON TABLE spectral_parameters TO cube_admin;
GRANT ALL ON TABLE spectral_parameters TO cube_admin_group;
GRANT SELECT ON TABLE spectral_parameters TO cube_user_group;


SET search_path = public, pg_catalog;

--
-- TOC entry 3149 (class 0 OID 0)
-- Dependencies: 177
-- Name: dataset; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE dataset FROM PUBLIC;
REVOKE ALL ON TABLE dataset FROM cube_admin;
GRANT ALL ON TABLE dataset TO cube_admin;
GRANT ALL ON TABLE dataset TO cube_admin_group;
GRANT SELECT ON TABLE dataset TO cube_user_group;


--
-- TOC entry 3151 (class 0 OID 0)
-- Dependencies: 178
-- Name: dataset_dimension; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE dataset_dimension FROM PUBLIC;
REVOKE ALL ON TABLE dataset_dimension FROM cube_admin;
GRANT ALL ON TABLE dataset_dimension TO cube_admin;
GRANT ALL ON TABLE dataset_dimension TO cube_admin_group;
GRANT SELECT ON TABLE dataset_dimension TO cube_user_group;


--
-- TOC entry 3153 (class 0 OID 0)
-- Dependencies: 179
-- Name: dataset_metadata; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE dataset_metadata FROM PUBLIC;
REVOKE ALL ON TABLE dataset_metadata FROM cube_admin;
GRANT ALL ON TABLE dataset_metadata TO cube_admin;
GRANT ALL ON TABLE dataset_metadata TO cube_admin_group;
GRANT SELECT ON TABLE dataset_metadata TO cube_user_group;


--
-- TOC entry 3155 (class 0 OID 0)
-- Dependencies: 180
-- Name: dataset_type; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE dataset_type FROM PUBLIC;
REVOKE ALL ON TABLE dataset_type FROM cube_admin;
GRANT ALL ON TABLE dataset_type TO cube_admin;
GRANT ALL ON TABLE dataset_type TO cube_admin_group;
GRANT SELECT ON TABLE dataset_type TO cube_user_group;


--
-- TOC entry 3157 (class 0 OID 0)
-- Dependencies: 181
-- Name: dataset_type_dimension; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE dataset_type_dimension FROM PUBLIC;
REVOKE ALL ON TABLE dataset_type_dimension FROM cube_admin;
GRANT ALL ON TABLE dataset_type_dimension TO cube_admin;
GRANT ALL ON TABLE dataset_type_dimension TO cube_admin_group;
GRANT SELECT ON TABLE dataset_type_dimension TO cube_user_group;


--
-- TOC entry 3159 (class 0 OID 0)
-- Dependencies: 182
-- Name: dataset_type_domain; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE dataset_type_domain FROM PUBLIC;
REVOKE ALL ON TABLE dataset_type_domain FROM cube_admin;
GRANT ALL ON TABLE dataset_type_domain TO cube_admin;
GRANT ALL ON TABLE dataset_type_domain TO cube_admin_group;
GRANT SELECT ON TABLE dataset_type_domain TO cube_user_group;


--
-- TOC entry 3161 (class 0 OID 0)
-- Dependencies: 183
-- Name: dataset_type_measurement_type; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE dataset_type_measurement_type FROM PUBLIC;
REVOKE ALL ON TABLE dataset_type_measurement_type FROM cube_admin;
GRANT ALL ON TABLE dataset_type_measurement_type TO cube_admin;
GRANT ALL ON TABLE dataset_type_measurement_type TO cube_admin_group;
GRANT SELECT ON TABLE dataset_type_measurement_type TO cube_user_group;


--
-- TOC entry 3163 (class 0 OID 0)
-- Dependencies: 184
-- Name: datatype; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE datatype FROM PUBLIC;
REVOKE ALL ON TABLE datatype FROM cube_admin;
GRANT ALL ON TABLE datatype TO cube_admin;
GRANT ALL ON TABLE datatype TO cube_admin_group;
GRANT SELECT ON TABLE datatype TO cube_user_group;


--
-- TOC entry 3165 (class 0 OID 0)
-- Dependencies: 185
-- Name: dimension; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE dimension FROM PUBLIC;
REVOKE ALL ON TABLE dimension FROM cube_admin;
GRANT ALL ON TABLE dimension TO cube_admin;
GRANT ALL ON TABLE dimension TO cube_admin_group;
GRANT SELECT ON TABLE dimension TO cube_user_group;


--
-- TOC entry 3167 (class 0 OID 0)
-- Dependencies: 186
-- Name: dimension_domain; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE dimension_domain FROM PUBLIC;
REVOKE ALL ON TABLE dimension_domain FROM cube_admin;
GRANT ALL ON TABLE dimension_domain TO cube_admin;
GRANT ALL ON TABLE dimension_domain TO cube_admin_group;
GRANT SELECT ON TABLE dimension_domain TO cube_user_group;


--
-- TOC entry 3169 (class 0 OID 0)
-- Dependencies: 187
-- Name: domain; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE domain FROM PUBLIC;
REVOKE ALL ON TABLE domain FROM cube_admin;
GRANT ALL ON TABLE domain TO cube_admin;
GRANT ALL ON TABLE domain TO cube_admin_group;
GRANT SELECT ON TABLE domain TO cube_user_group;


--
-- TOC entry 3171 (class 0 OID 0)
-- Dependencies: 188
-- Name: indexing_type; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE indexing_type FROM PUBLIC;
REVOKE ALL ON TABLE indexing_type FROM cube_admin;
GRANT ALL ON TABLE indexing_type TO cube_admin;
GRANT ALL ON TABLE indexing_type TO cube_admin_group;
GRANT SELECT ON TABLE indexing_type TO cube_user_group;


--
-- TOC entry 3173 (class 0 OID 0)
-- Dependencies: 189
-- Name: reference_system; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE reference_system FROM PUBLIC;
REVOKE ALL ON TABLE reference_system FROM cube_admin;
GRANT ALL ON TABLE reference_system TO cube_admin;
GRANT ALL ON TABLE reference_system TO cube_admin_group;
GRANT SELECT ON TABLE reference_system TO cube_user_group;


--
-- TOC entry 3175 (class 0 OID 0)
-- Dependencies: 190
-- Name: reference_system_indexing; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE reference_system_indexing FROM PUBLIC;
REVOKE ALL ON TABLE reference_system_indexing FROM cube_admin;
GRANT ALL ON TABLE reference_system_indexing TO cube_admin;
GRANT ALL ON TABLE reference_system_indexing TO cube_admin_group;
GRANT SELECT ON TABLE reference_system_indexing TO cube_user_group;


--
-- TOC entry 3178 (class 0 OID 0)
-- Dependencies: 191
-- Name: storage_type; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE storage_type FROM PUBLIC;
REVOKE ALL ON TABLE storage_type FROM cube_admin;
GRANT ALL ON TABLE storage_type TO cube_admin;
GRANT ALL ON TABLE storage_type TO cube_admin_group;
GRANT SELECT ON TABLE storage_type TO cube_user_group;


--
-- TOC entry 3181 (class 0 OID 0)
-- Dependencies: 192
-- Name: storage_type_dimension; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE storage_type_dimension FROM PUBLIC;
REVOKE ALL ON TABLE storage_type_dimension FROM cube_admin;
GRANT ALL ON TABLE storage_type_dimension TO cube_admin;
GRANT ALL ON TABLE storage_type_dimension TO cube_admin_group;
GRANT SELECT ON TABLE storage_type_dimension TO cube_user_group;


--
-- TOC entry 3182 (class 0 OID 0)
-- Dependencies: 193
-- Name: storage_type_dimension_view; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE storage_type_dimension_view FROM PUBLIC;
REVOKE ALL ON TABLE storage_type_dimension_view FROM cube_admin;
GRANT ALL ON TABLE storage_type_dimension_view TO cube_admin;
GRANT ALL ON TABLE storage_type_dimension_view TO cube_admin_group;
GRANT SELECT ON TABLE storage_type_dimension_view TO cube_user_group;


--
-- TOC entry 3183 (class 0 OID 0)
-- Dependencies: 194
-- Name: dimension_indices_view; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE dimension_indices_view FROM PUBLIC;
REVOKE ALL ON TABLE dimension_indices_view FROM cube_admin;
GRANT ALL ON TABLE dimension_indices_view TO cube_admin;
GRANT ALL ON TABLE dimension_indices_view TO cube_admin_group;
GRANT SELECT ON TABLE dimension_indices_view TO cube_user_group;


--
-- TOC entry 3184 (class 0 OID 0)
-- Dependencies: 195
-- Name: property; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE property FROM PUBLIC;
REVOKE ALL ON TABLE property FROM cube_admin;
GRANT ALL ON TABLE property TO cube_admin;
GRANT ALL ON TABLE property TO cube_admin_group;
GRANT SELECT ON TABLE property TO cube_user_group;


--
-- TOC entry 3186 (class 0 OID 0)
-- Dependencies: 196
-- Name: storage_type_dimension_property; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE storage_type_dimension_property FROM PUBLIC;
REVOKE ALL ON TABLE storage_type_dimension_property FROM cube_admin;
GRANT ALL ON TABLE storage_type_dimension_property TO cube_admin;
GRANT ALL ON TABLE storage_type_dimension_property TO cube_admin_group;
GRANT SELECT ON TABLE storage_type_dimension_property TO cube_user_group;


--
-- TOC entry 3187 (class 0 OID 0)
-- Dependencies: 197
-- Name: dimension_properties_view; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE dimension_properties_view FROM PUBLIC;
REVOKE ALL ON TABLE dimension_properties_view FROM cube_admin;
GRANT ALL ON TABLE dimension_properties_view TO cube_admin;
GRANT ALL ON TABLE dimension_properties_view TO cube_admin_group;
GRANT SELECT ON TABLE dimension_properties_view TO cube_user_group;


--
-- TOC entry 3189 (class 0 OID 0)
-- Dependencies: 198
-- Name: instrument; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE instrument FROM PUBLIC;
REVOKE ALL ON TABLE instrument FROM cube_admin;
GRANT ALL ON TABLE instrument TO cube_admin;
GRANT ALL ON TABLE instrument TO cube_admin_group;
GRANT SELECT ON TABLE instrument TO cube_user_group;


--
-- TOC entry 3191 (class 0 OID 0)
-- Dependencies: 199
-- Name: instrument_type; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE instrument_type FROM PUBLIC;
REVOKE ALL ON TABLE instrument_type FROM cube_admin;
GRANT ALL ON TABLE instrument_type TO cube_admin;
GRANT ALL ON TABLE instrument_type TO cube_admin_group;
GRANT SELECT ON TABLE instrument_type TO cube_user_group;


--
-- TOC entry 3193 (class 0 OID 0)
-- Dependencies: 200
-- Name: measurement_metatype; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE measurement_metatype FROM PUBLIC;
REVOKE ALL ON TABLE measurement_metatype FROM cube_admin;
GRANT ALL ON TABLE measurement_metatype TO cube_admin;
GRANT ALL ON TABLE measurement_metatype TO cube_admin_group;
GRANT SELECT ON TABLE measurement_metatype TO cube_user_group;


--
-- TOC entry 3195 (class 0 OID 0)
-- Dependencies: 201
-- Name: measurement_type; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE measurement_type FROM PUBLIC;
REVOKE ALL ON TABLE measurement_type FROM cube_admin;
GRANT ALL ON TABLE measurement_type TO cube_admin;
GRANT ALL ON TABLE measurement_type TO cube_admin_group;
GRANT SELECT ON TABLE measurement_type TO cube_user_group;


--
-- TOC entry 3197 (class 0 OID 0)
-- Dependencies: 202
-- Name: observation; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE observation FROM PUBLIC;
REVOKE ALL ON TABLE observation FROM cube_admin;
GRANT ALL ON TABLE observation TO cube_admin;
GRANT ALL ON TABLE observation TO cube_admin_group;
GRANT SELECT ON TABLE observation TO cube_user_group;


--
-- TOC entry 3199 (class 0 OID 0)
-- Dependencies: 203
-- Name: observation_type; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE observation_type FROM PUBLIC;
REVOKE ALL ON TABLE observation_type FROM cube_admin;
GRANT ALL ON TABLE observation_type TO cube_admin;
GRANT ALL ON TABLE observation_type TO cube_admin_group;
GRANT SELECT ON TABLE observation_type TO cube_user_group;


--
-- TOC entry 3201 (class 0 OID 0)
-- Dependencies: 204
-- Name: platform; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE platform FROM PUBLIC;
REVOKE ALL ON TABLE platform FROM cube_admin;
GRANT ALL ON TABLE platform TO cube_admin;
GRANT ALL ON TABLE platform TO cube_admin_group;
GRANT SELECT ON TABLE platform TO cube_user_group;


--
-- TOC entry 3203 (class 0 OID 0)
-- Dependencies: 205
-- Name: platform_type; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE platform_type FROM PUBLIC;
REVOKE ALL ON TABLE platform_type FROM cube_admin;
GRANT ALL ON TABLE platform_type TO cube_admin;
GRANT ALL ON TABLE platform_type TO cube_admin_group;
GRANT SELECT ON TABLE platform_type TO cube_user_group;


--
-- TOC entry 3205 (class 0 OID 0)
-- Dependencies: 206
-- Name: storage; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE storage FROM PUBLIC;
REVOKE ALL ON TABLE storage FROM cube_admin;
GRANT ALL ON TABLE storage TO cube_admin;
GRANT ALL ON TABLE storage TO cube_admin_group;
GRANT SELECT ON TABLE storage TO cube_user_group;


--
-- TOC entry 3207 (class 0 OID 0)
-- Dependencies: 207
-- Name: storage_dataset; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE storage_dataset FROM PUBLIC;
REVOKE ALL ON TABLE storage_dataset FROM cube_admin;
GRANT ALL ON TABLE storage_dataset TO cube_admin;
GRANT ALL ON TABLE storage_dataset TO cube_admin_group;
GRANT SELECT ON TABLE storage_dataset TO cube_user_group;


--
-- TOC entry 3209 (class 0 OID 0)
-- Dependencies: 208
-- Name: storage_dimension; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE storage_dimension FROM PUBLIC;
REVOKE ALL ON TABLE storage_dimension FROM cube_admin;
GRANT ALL ON TABLE storage_dimension TO cube_admin;
GRANT ALL ON TABLE storage_dimension TO cube_admin_group;
GRANT SELECT ON TABLE storage_dimension TO cube_user_group;


--
-- TOC entry 3211 (class 0 OID 0)
-- Dependencies: 209
-- Name: storage_type_measurement_type; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE storage_type_measurement_type FROM PUBLIC;
REVOKE ALL ON TABLE storage_type_measurement_type FROM cube_admin;
GRANT ALL ON TABLE storage_type_measurement_type TO cube_admin;
GRANT ALL ON TABLE storage_type_measurement_type TO cube_admin_group;
GRANT SELECT ON TABLE storage_type_measurement_type TO cube_user_group;


-- Completed on 2015-07-09 13:23:41

--
-- PostgreSQL database dump complete
--

