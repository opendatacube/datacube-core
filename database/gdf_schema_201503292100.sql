--
-- PostgreSQL database dump
--

-- Dumped from database version 9.3.6
-- Dumped by pg_dump version 9.3.6
-- Started on 2015-03-29 20:59:09 AEDT

SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;

--
-- TOC entry 3866 (class 1262 OID 986027)
-- Dependencies: 3865
-- Name: gdf; Type: COMMENT; Schema: -; Owner: cube_admin
--

COMMENT ON DATABASE gdf IS 'Draft General Data Framework Development Database';


--
-- TOC entry 6 (class 2615 OID 986028)
-- Name: earth_observation; Type: SCHEMA; Schema: -; Owner: cube_admin
--

CREATE SCHEMA earth_observation;


ALTER SCHEMA earth_observation OWNER TO cube_admin;

--
-- TOC entry 7 (class 2615 OID 986029)
-- Name: topology; Type: SCHEMA; Schema: -; Owner: cube_admin
--

CREATE SCHEMA topology;


ALTER SCHEMA topology OWNER TO cube_admin;

--
-- TOC entry 9 (class 2615 OID 986030)
-- Name: ztmp; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA ztmp;


ALTER SCHEMA ztmp OWNER TO postgres;

--
-- TOC entry 3869 (class 0 OID 0)
-- Dependencies: 9
-- Name: SCHEMA ztmp; Type: COMMENT; Schema: -; Owner: postgres
--

COMMENT ON SCHEMA ztmp IS 'Temporary schema';


--
-- TOC entry 232 (class 3079 OID 11929)
-- Name: plpgsql; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;


--
-- TOC entry 3871 (class 0 OID 0)
-- Dependencies: 232
-- Name: EXTENSION plpgsql; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';


--
-- TOC entry 231 (class 3079 OID 986031)
-- Name: adminpack; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS adminpack WITH SCHEMA pg_catalog;


--
-- TOC entry 3872 (class 0 OID 0)
-- Dependencies: 231
-- Name: EXTENSION adminpack; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION adminpack IS 'administrative functions for PostgreSQL';


--
-- TOC entry 234 (class 3079 OID 986040)
-- Name: postgis; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS postgis WITH SCHEMA public;


--
-- TOC entry 3873 (class 0 OID 0)
-- Dependencies: 234
-- Name: EXTENSION postgis; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION postgis IS 'PostGIS geometry, geography, and raster spatial types and functions';


--
-- TOC entry 233 (class 3079 OID 987326)
-- Name: postgis_topology; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS postgis_topology WITH SCHEMA topology;


--
-- TOC entry 3874 (class 0 OID 0)
-- Dependencies: 233
-- Name: EXTENSION postgis_topology; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION postgis_topology IS 'PostGIS topology spatial types and functions';


SET search_path = public, pg_catalog;

--
-- TOC entry 1815 (class 1247 OID 987478)
-- Name: attribute_value_type; Type: TYPE; Schema: public; Owner: cube_admin
--

CREATE TYPE attribute_value_type AS (
	attribute_type_id bigint,
	attribute_id bigint,
	attribute_value_id bigint
);


ALTER TYPE public.attribute_value_type OWNER TO cube_admin;

--
-- TOC entry 1818 (class 1247 OID 987481)
-- Name: category_id_level_type; Type: TYPE; Schema: public; Owner: cube_admin
--

CREATE TYPE category_id_level_type AS (
	category_type_id bigint,
	category_id bigint,
	level integer
);


ALTER TYPE public.category_id_level_type OWNER TO cube_admin;

--
-- TOC entry 1821 (class 1247 OID 987484)
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
-- TOC entry 195 (class 1259 OID 987485)
-- Name: spectral_parameters; Type: TABLE; Schema: earth_observation; Owner: cube_admin; Tablespace: 
--

CREATE TABLE spectral_parameters (
    measurement_metatype_id bigint NOT NULL,
    measurement_type_id bigint NOT NULL
);


ALTER TABLE earth_observation.spectral_parameters OWNER TO cube_admin;

--
-- TOC entry 3875 (class 0 OID 0)
-- Dependencies: 195
-- Name: TABLE spectral_parameters; Type: COMMENT; Schema: earth_observation; Owner: cube_admin
--

COMMENT ON TABLE spectral_parameters IS 'Configuration: Spectral band parameters';


SET search_path = public, pg_catalog;

--
-- TOC entry 196 (class 1259 OID 987488)
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
-- TOC entry 3877 (class 0 OID 0)
-- Dependencies: 196
-- Name: TABLE dataset; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE dataset IS 'Data: Source dataset (file) ingested.
An example would be a dataset for a particular NBAR Landsat scene.';


--
-- TOC entry 197 (class 1259 OID 987491)
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
-- TOC entry 3879 (class 0 OID 0)
-- Dependencies: 197
-- Name: TABLE dataset_dimension; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE dataset_dimension IS 'Data: Dimensional parameters for each source dataset.
Each dataset/dimension will have specific max/min/indexing values showing the range covered by the dataset in that particular dimension.';


--
-- TOC entry 198 (class 1259 OID 987494)
-- Name: dataset_metadata; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE dataset_metadata (
    dataset_type_id bigint NOT NULL,
    dataset_id bigint NOT NULL,
    metadata_xml xml NOT NULL
);


ALTER TABLE public.dataset_metadata OWNER TO cube_admin;

--
-- TOC entry 3881 (class 0 OID 0)
-- Dependencies: 198
-- Name: TABLE dataset_metadata; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE dataset_metadata IS 'Data: Lookup table for dataset-level metadata (one:one)';


--
-- TOC entry 199 (class 1259 OID 987500)
-- Name: dataset_type; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE dataset_type (
    dataset_type_id bigint NOT NULL,
    dataset_type_name character varying(254)
);


ALTER TABLE public.dataset_type OWNER TO cube_admin;

--
-- TOC entry 3883 (class 0 OID 0)
-- Dependencies: 199
-- Name: TABLE dataset_type; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE dataset_type IS 'Configuration: Type of source dataset (processing level)';


--
-- TOC entry 200 (class 1259 OID 987503)
-- Name: dataset_type_dimension; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE dataset_type_dimension (
    dataset_type_id bigint NOT NULL,
    domain_id bigint NOT NULL,
    dimension_id bigint NOT NULL
);


ALTER TABLE public.dataset_type_dimension OWNER TO cube_admin;

--
-- TOC entry 3885 (class 0 OID 0)
-- Dependencies: 200
-- Name: TABLE dataset_type_dimension; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE dataset_type_dimension IS 'Configuration: Association between dataset type and dimensions. Used to define dimensionality of source dataset types';


--
-- TOC entry 201 (class 1259 OID 987506)
-- Name: dataset_type_domain; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE dataset_type_domain (
    dataset_type_id bigint NOT NULL,
    domain_id bigint NOT NULL,
    reference_system_id bigint
);


ALTER TABLE public.dataset_type_domain OWNER TO cube_admin;

--
-- TOC entry 3887 (class 0 OID 0)
-- Dependencies: 201
-- Name: TABLE dataset_type_domain; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE dataset_type_domain IS 'Configuration: Association between dataset types and domains (many-many).
Used to define which domains cover a given dataset type';


--
-- TOC entry 202 (class 1259 OID 987509)
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
-- TOC entry 3889 (class 0 OID 0)
-- Dependencies: 202
-- Name: TABLE dataset_type_measurement_type; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE dataset_type_measurement_type IS 'Configuration: Associations between dataset types and measurement types (one-many)
e.g. associations between Landsat 7 NBAR and specific surface-reflectance corrected Landsat 7 bands';


--
-- TOC entry 203 (class 1259 OID 987512)
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
-- TOC entry 3891 (class 0 OID 0)
-- Dependencies: 203
-- Name: TABLE datatype; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE datatype IS 'Configuration: Lookup table for measurement_type datatypes.';


--
-- TOC entry 204 (class 1259 OID 987515)
-- Name: dimension; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE dimension (
    dimension_id bigint NOT NULL,
    dimension_name character varying(50) NOT NULL,
    dimension_tag character varying(8) NOT NULL
);


ALTER TABLE public.dimension OWNER TO cube_admin;

--
-- TOC entry 3893 (class 0 OID 0)
-- Dependencies: 204
-- Name: TABLE dimension; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE dimension IS 'Configuration: Dimensions for n-dimensional data structures, e.g. x,y,z,t';


--
-- TOC entry 205 (class 1259 OID 987518)
-- Name: dimension_domain; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE dimension_domain (
    domain_id bigint NOT NULL,
    dimension_id bigint NOT NULL
);


ALTER TABLE public.dimension_domain OWNER TO cube_admin;

--
-- TOC entry 3895 (class 0 OID 0)
-- Dependencies: 205
-- Name: TABLE dimension_domain; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE dimension_domain IS 'Configuration: Many-many  mapping between dimensions and domains to allow multiple dimensions to be included in multiple domains.
For example, the z dimension could be managed in a Z-spatial domain, or in an XYZ-spatial domain.';


--
-- TOC entry 206 (class 1259 OID 987521)
-- Name: domain; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE domain (
    domain_id bigint NOT NULL,
    domain_name character varying(16),
    domain_tag character varying(16)
);


ALTER TABLE public.domain OWNER TO cube_admin;

--
-- TOC entry 3897 (class 0 OID 0)
-- Dependencies: 206
-- Name: TABLE domain; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE domain IS 'Configuration: Domain groupings of dimensions (e.g. spectral, spatial XY, spatial XYZ, temporal)';


--
-- TOC entry 207 (class 1259 OID 987524)
-- Name: indexing_type; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE indexing_type (
    indexing_type_id smallint NOT NULL,
    indexing_type_name character varying(128)
);


ALTER TABLE public.indexing_type OWNER TO cube_admin;

--
-- TOC entry 3899 (class 0 OID 0)
-- Dependencies: 207
-- Name: TABLE indexing_type; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE indexing_type IS 'Configuration: Lookup table to manage what kind of indexing to apply to a given dimension';


--
-- TOC entry 208 (class 1259 OID 987527)
-- Name: ndarray_type; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE ndarray_type (
    ndarray_type_id bigint NOT NULL,
    ndarray_type_name character varying(254),
    ndarray_type_tag character varying(16)
);


ALTER TABLE public.ndarray_type OWNER TO cube_admin;

--
-- TOC entry 3901 (class 0 OID 0)
-- Dependencies: 208
-- Name: TABLE ndarray_type; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE ndarray_type IS 'Configuration: ndarray parameter lookup table. Used to manage different ndarray_types';


--
-- TOC entry 209 (class 1259 OID 987530)
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
-- TOC entry 3903 (class 0 OID 0)
-- Dependencies: 209
-- Name: TABLE ndarray_type_dimension; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE ndarray_type_dimension IS 'Configuration: Association between attribute set and dimensions. Used to define dimensionality of ndarrays using a given attribute set';


--
-- TOC entry 210 (class 1259 OID 987536)
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
-- TOC entry 3905 (class 0 OID 0)
-- Dependencies: 210
-- Name: TABLE reference_system; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE reference_system IS 'Configuration: Coordinate reference systems for aplication to specific domains.
e.g. EPSG:4326, seconds since 1/1/1970 0:00, etc.';


--
-- TOC entry 211 (class 1259 OID 987539)
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
-- TOC entry 212 (class 1259 OID 987547)
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
-- TOC entry 3908 (class 0 OID 0)
-- Dependencies: 212
-- Name: TABLE reference_system_indexing; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE reference_system_indexing IS 'Configuration: Optional non-linear indexing for dimension in a given domain.
e.g. A spectral dimension containing multple bands needs to be indexed by band number, and each band number can be associated with a given measurement_type.';


--
-- TOC entry 213 (class 1259 OID 987550)
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
-- TOC entry 214 (class 1259 OID 987554)
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
-- TOC entry 215 (class 1259 OID 987557)
-- Name: property; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE property (
    property_id bigint NOT NULL,
    property_name character varying(32),
    datatype_id smallint
);


ALTER TABLE public.property OWNER TO cube_admin;

--
-- TOC entry 216 (class 1259 OID 987560)
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
-- TOC entry 217 (class 1259 OID 987565)
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
-- TOC entry 3914 (class 0 OID 0)
-- Dependencies: 217
-- Name: TABLE instrument; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE instrument IS 'Configuration: Instrument used to gather observations.
An example would be the ETM+ sensor on the Landsat 7 platform';


--
-- TOC entry 218 (class 1259 OID 987568)
-- Name: instrument_type; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE instrument_type (
    instrument_type_id bigint NOT NULL,
    instrument_type_name character varying(128)
);


ALTER TABLE public.instrument_type OWNER TO cube_admin;

--
-- TOC entry 3916 (class 0 OID 0)
-- Dependencies: 218
-- Name: TABLE instrument_type; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE instrument_type IS 'Configuration: Lookup table for instrument category';


--
-- TOC entry 219 (class 1259 OID 987571)
-- Name: measurement_metatype; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE measurement_metatype (
    measurement_metatype_id bigint NOT NULL,
    measurement_metatype_name character varying(254)
);


ALTER TABLE public.measurement_metatype OWNER TO cube_admin;

--
-- TOC entry 3918 (class 0 OID 0)
-- Dependencies: 219
-- Name: TABLE measurement_metatype; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE measurement_metatype IS 'Configuration: General type of measurement (e.g. spectral band)';


--
-- TOC entry 220 (class 1259 OID 987574)
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
-- TOC entry 3920 (class 0 OID 0)
-- Dependencies: 220
-- Name: TABLE measurement_type; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE measurement_type IS 'Configuration: Description of measurement(s) held in n-dimensional data structures: e.g. bands';


--
-- TOC entry 221 (class 1259 OID 987577)
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
-- TOC entry 3922 (class 0 OID 0)
-- Dependencies: 221
-- Name: TABLE ndarray; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE ndarray IS 'Data: n-dimensional data structure instances';


--
-- TOC entry 222 (class 1259 OID 987580)
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
-- TOC entry 3924 (class 0 OID 0)
-- Dependencies: 222
-- Name: TABLE ndarray_dataset; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE ndarray_dataset IS 'Data: Association between ndarray and dataset instances (many-many)';


--
-- TOC entry 223 (class 1259 OID 987583)
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
-- TOC entry 3926 (class 0 OID 0)
-- Dependencies: 223
-- Name: TABLE ndarray_dimension; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE ndarray_dimension IS 'Data: Association between ndarray and dimensions. Used to define attributes for each dimension in ndarray instances';


--
-- TOC entry 224 (class 1259 OID 987586)
-- Name: ndarray_footprint; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE ndarray_footprint (
    ndarray_type_id bigint NOT NULL,
    ndarray_footprint_id bigint NOT NULL,
    bounding_box geometry
);


ALTER TABLE public.ndarray_footprint OWNER TO cube_admin;

--
-- TOC entry 3928 (class 0 OID 0)
-- Dependencies: 224
-- Name: TABLE ndarray_footprint; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE ndarray_footprint IS 'Data: Spatial footprint for tiles with a given set of indices in the XY dimensions';


--
-- TOC entry 225 (class 1259 OID 987592)
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
-- TOC entry 3930 (class 0 OID 0)
-- Dependencies: 225
-- Name: TABLE ndarray_footprint_dimension; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE ndarray_footprint_dimension IS 'Data: Association between ndarray footprint and dimensions';


--
-- TOC entry 226 (class 1259 OID 987595)
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
-- TOC entry 3932 (class 0 OID 0)
-- Dependencies: 226
-- Name: TABLE ndarray_type_measurement_type; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE ndarray_type_measurement_type IS 'Configuration: Associations between n-dimensional data structure types and measurement types (many-many)
e.g. associations between Landsat 7 arrays and Landsat 7 bands if they are stored in separate variables';


--
-- TOC entry 227 (class 1259 OID 987598)
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
-- TOC entry 3934 (class 0 OID 0)
-- Dependencies: 227
-- Name: TABLE observation; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE observation IS 'Data: Source observation for datasets.
Analagous to old "acquisition" table in AGDC version 0 DB';


--
-- TOC entry 228 (class 1259 OID 987601)
-- Name: observation_type; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE observation_type (
    observation_type_id bigint NOT NULL,
    observation_type_name character varying(254)
);


ALTER TABLE public.observation_type OWNER TO cube_admin;

--
-- TOC entry 3936 (class 0 OID 0)
-- Dependencies: 228
-- Name: TABLE observation_type; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE observation_type IS 'Configuration: Type of source observation';


--
-- TOC entry 229 (class 1259 OID 987604)
-- Name: platform; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE platform (
    platform_type_id bigint NOT NULL,
    platform_id bigint NOT NULL,
    platform_name character varying(128)
);


ALTER TABLE public.platform OWNER TO cube_admin;

--
-- TOC entry 3938 (class 0 OID 0)
-- Dependencies: 229
-- Name: TABLE platform; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE platform IS 'Configuration: Platform on which instrument is mounted.
An example would be a specific satellite such as Landsat 7';


--
-- TOC entry 230 (class 1259 OID 987607)
-- Name: platform_type; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE platform_type (
    platform_type_id bigint NOT NULL,
    platform_type_name character varying(128)
);


ALTER TABLE public.platform_type OWNER TO cube_admin;

--
-- TOC entry 3940 (class 0 OID 0)
-- Dependencies: 230
-- Name: TABLE platform_type; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE platform_type IS 'Configuration: Lookup table for platform category
e.g. Satellite or Ship';


SET search_path = earth_observation, pg_catalog;

--
-- TOC entry 3825 (class 0 OID 987485)
-- Dependencies: 195
-- Data for Name: spectral_parameters; Type: TABLE DATA; Schema: earth_observation; Owner: cube_admin
--



SET search_path = public, pg_catalog;

--
-- TOC entry 3826 (class 0 OID 987488)
-- Dependencies: 196
-- Data for Name: dataset; Type: TABLE DATA; Schema: public; Owner: cube_admin
--



--
-- TOC entry 3827 (class 0 OID 987491)
-- Dependencies: 197
-- Data for Name: dataset_dimension; Type: TABLE DATA; Schema: public; Owner: cube_admin
--



--
-- TOC entry 3828 (class 0 OID 987494)
-- Dependencies: 198
-- Data for Name: dataset_metadata; Type: TABLE DATA; Schema: public; Owner: cube_admin
--



--
-- TOC entry 3829 (class 0 OID 987500)
-- Dependencies: 199
-- Data for Name: dataset_type; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO dataset_type VALUES (1, 'L1T');
INSERT INTO dataset_type VALUES (2, 'NBAR');
INSERT INTO dataset_type VALUES (3, 'PQ');
INSERT INTO dataset_type VALUES (4, 'FC');


--
-- TOC entry 3830 (class 0 OID 987503)
-- Dependencies: 200
-- Data for Name: dataset_type_dimension; Type: TABLE DATA; Schema: public; Owner: cube_admin
--



--
-- TOC entry 3831 (class 0 OID 987506)
-- Dependencies: 201
-- Data for Name: dataset_type_domain; Type: TABLE DATA; Schema: public; Owner: cube_admin
--



--
-- TOC entry 3832 (class 0 OID 987509)
-- Dependencies: 202
-- Data for Name: dataset_type_measurement_type; Type: TABLE DATA; Schema: public; Owner: cube_admin
--



--
-- TOC entry 3833 (class 0 OID 987512)
-- Dependencies: 203
-- Data for Name: datatype; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO datatype VALUES (3, 'string', NULL, NULL, NULL);
INSERT INTO datatype VALUES (1, 'int16', 'int16', 'int16', 'i2');
INSERT INTO datatype VALUES (2, 'float32', 'float32', 'float32', 'f4');


--
-- TOC entry 3834 (class 0 OID 987515)
-- Dependencies: 204
-- Data for Name: dimension; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO dimension VALUES (1, 'longitude', 'X');
INSERT INTO dimension VALUES (2, 'latitude', 'Y');
INSERT INTO dimension VALUES (3, 'height/depth', 'Z');
INSERT INTO dimension VALUES (5, 'spectral', 'LAMBDA');
INSERT INTO dimension VALUES (4, 'time', 'T');


--
-- TOC entry 3835 (class 0 OID 987518)
-- Dependencies: 205
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
-- TOC entry 3836 (class 0 OID 987521)
-- Dependencies: 206
-- Data for Name: domain; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO domain VALUES (1, 'Spatial XY', 'XY');
INSERT INTO domain VALUES (2, 'Spatial Z', 'Z');
INSERT INTO domain VALUES (3, 'Temporal', 'T');
INSERT INTO domain VALUES (4, 'Spectral', 'B');
INSERT INTO domain VALUES (5, 'Spatial XYZ', 'XYZ');


--
-- TOC entry 3837 (class 0 OID 987524)
-- Dependencies: 207
-- Data for Name: indexing_type; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO indexing_type VALUES (1, 'regular');
INSERT INTO indexing_type VALUES (2, 'irregular');
INSERT INTO indexing_type VALUES (3, 'fixed');


--
-- TOC entry 3847 (class 0 OID 987565)
-- Dependencies: 217
-- Data for Name: instrument; Type: TABLE DATA; Schema: public; Owner: cube_admin
--



--
-- TOC entry 3848 (class 0 OID 987568)
-- Dependencies: 218
-- Data for Name: instrument_type; Type: TABLE DATA; Schema: public; Owner: cube_admin
--



--
-- TOC entry 3849 (class 0 OID 987571)
-- Dependencies: 219
-- Data for Name: measurement_metatype; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO measurement_metatype VALUES (10, 'Multi-band Spectral Radiance');
INSERT INTO measurement_metatype VALUES (1, 'Spectral Radiance (Single Band)');


--
-- TOC entry 3850 (class 0 OID 987574)
-- Dependencies: 220
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
-- TOC entry 3851 (class 0 OID 987577)
-- Dependencies: 221
-- Data for Name: ndarray; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO ndarray VALUES (5, 1, 0, 'test_location', NULL, NULL);


--
-- TOC entry 3852 (class 0 OID 987580)
-- Dependencies: 222
-- Data for Name: ndarray_dataset; Type: TABLE DATA; Schema: public; Owner: cube_admin
--



--
-- TOC entry 3853 (class 0 OID 987583)
-- Dependencies: 223
-- Data for Name: ndarray_dimension; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO ndarray_dimension VALUES (5, 1, 0, 1, 1, 140);
INSERT INTO ndarray_dimension VALUES (5, 1, 0, 1, 2, -35);
INSERT INTO ndarray_dimension VALUES (5, 1, 0, 3, 4, 2010);


--
-- TOC entry 3854 (class 0 OID 987586)
-- Dependencies: 224
-- Data for Name: ndarray_footprint; Type: TABLE DATA; Schema: public; Owner: cube_admin
--



--
-- TOC entry 3855 (class 0 OID 987592)
-- Dependencies: 225
-- Data for Name: ndarray_footprint_dimension; Type: TABLE DATA; Schema: public; Owner: cube_admin
--



--
-- TOC entry 3838 (class 0 OID 987527)
-- Dependencies: 208
-- Data for Name: ndarray_type; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO ndarray_type VALUES (5, 'Landsat 5 TM ARG-25', 'LS5TM');
INSERT INTO ndarray_type VALUES (7, 'Landsat 7 ETM ARG-25', 'LS7ETM');
INSERT INTO ndarray_type VALUES (8, 'Landsat 8 OLI ARG-25', 'LS8OLI');
INSERT INTO ndarray_type VALUES (82, 'Landsat 8 TIRS', 'LS8TIRS');
INSERT INTO ndarray_type VALUES (50, 'Landsat 5 TM ARG-25 with spectral dimension', 'LS5TM-SD');


--
-- TOC entry 3839 (class 0 OID 987530)
-- Dependencies: 209
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
-- TOC entry 3844 (class 0 OID 987554)
-- Dependencies: 214
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
-- TOC entry 3856 (class 0 OID 987595)
-- Dependencies: 226
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
-- TOC entry 3857 (class 0 OID 987598)
-- Dependencies: 227
-- Data for Name: observation; Type: TABLE DATA; Schema: public; Owner: cube_admin
--



--
-- TOC entry 3858 (class 0 OID 987601)
-- Dependencies: 228
-- Data for Name: observation_type; Type: TABLE DATA; Schema: public; Owner: cube_admin
--



--
-- TOC entry 3859 (class 0 OID 987604)
-- Dependencies: 229
-- Data for Name: platform; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO platform VALUES (1, 5, 'Landsat 5');
INSERT INTO platform VALUES (1, 7, 'Landsat 7');
INSERT INTO platform VALUES (1, 8, 'Landsat 8');


--
-- TOC entry 3860 (class 0 OID 987607)
-- Dependencies: 230
-- Data for Name: platform_type; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO platform_type VALUES (1, 'Satellite');


--
-- TOC entry 3845 (class 0 OID 987557)
-- Dependencies: 215
-- Data for Name: property; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO property VALUES (4, 'axis', 3);
INSERT INTO property VALUES (2, 'standard_name', 3);
INSERT INTO property VALUES (1, 'long_name', 3);
INSERT INTO property VALUES (3, 'units', 3);
INSERT INTO property VALUES (5, 'calendar', 3);


--
-- TOC entry 3840 (class 0 OID 987536)
-- Dependencies: 210
-- Data for Name: reference_system; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO reference_system VALUES (4326, 'Unprojected WGS84 (Lat-long)', 'degrees', 'EPSG:4326', NULL);
INSERT INTO reference_system VALUES (3, 'Australian Height Datum (AHD)', 'metres', 'AHD', NULL);
INSERT INTO reference_system VALUES (4, 'Seconds since 1/1/1970 0:00', 'seconds', 'SSE', NULL);
INSERT INTO reference_system VALUES (70, 'Landsat 7 Spectral Bands', 'band', 'LS7', NULL);
INSERT INTO reference_system VALUES (80, 'Landsat 8 Band', 'band', 'LS8', NULL);
INSERT INTO reference_system VALUES (50, 'Landsat 5 Reflectance Bands', 'band', 'LS5', NULL);


--
-- TOC entry 3842 (class 0 OID 987547)
-- Dependencies: 212
-- Data for Name: reference_system_indexing; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO reference_system_indexing VALUES (50, 0, 'Band 1 - Visible Blue', 1, 51);
INSERT INTO reference_system_indexing VALUES (50, 1, 'Band 2 - Visible Green', 1, 52);
INSERT INTO reference_system_indexing VALUES (50, 2, 'Band 3 - Visible Red', 1, 53);
INSERT INTO reference_system_indexing VALUES (50, 3, 'Band 4 - Near Infrared', 1, 54);
INSERT INTO reference_system_indexing VALUES (50, 4, 'Band 5 - Middle Infrared 1', 1, 55);
INSERT INTO reference_system_indexing VALUES (50, 5, 'Band 7 - Middle Infrared 2', 1, 57);


--
-- TOC entry 3518 (class 0 OID 986308)
-- Dependencies: 174
-- Data for Name: spatial_ref_sys; Type: TABLE DATA; Schema: public; Owner: cube_admin
--



SET search_path = topology, pg_catalog;

--
-- TOC entry 3517 (class 0 OID 987342)
-- Dependencies: 188
-- Data for Name: layer; Type: TABLE DATA; Schema: topology; Owner: cube_admin
--



--
-- TOC entry 3516 (class 0 OID 987329)
-- Dependencies: 187
-- Data for Name: topology; Type: TABLE DATA; Schema: topology; Owner: cube_admin
--



SET search_path = earth_observation, pg_catalog;

--
-- TOC entry 3525 (class 2606 OID 987611)
-- Name: pk_spectral_parameters; Type: CONSTRAINT; Schema: earth_observation; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY spectral_parameters
    ADD CONSTRAINT pk_spectral_parameters PRIMARY KEY (measurement_metatype_id, measurement_type_id);


SET search_path = public, pg_catalog;

--
-- TOC entry 3529 (class 2606 OID 987613)
-- Name: pk_dataset; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dataset
    ADD CONSTRAINT pk_dataset PRIMARY KEY (dataset_type_id, dataset_id);


--
-- TOC entry 3533 (class 2606 OID 987615)
-- Name: pk_dataset_dimension; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dataset_dimension
    ADD CONSTRAINT pk_dataset_dimension PRIMARY KEY (dataset_type_id, dataset_id, domain_id, dimension_id);


--
-- TOC entry 3536 (class 2606 OID 987617)
-- Name: pk_dataset_metadata; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dataset_metadata
    ADD CONSTRAINT pk_dataset_metadata PRIMARY KEY (dataset_type_id, dataset_id);


--
-- TOC entry 3538 (class 2606 OID 987619)
-- Name: pk_dataset_type; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dataset_type
    ADD CONSTRAINT pk_dataset_type PRIMARY KEY (dataset_type_id);


--
-- TOC entry 3544 (class 2606 OID 987621)
-- Name: pk_dataset_type_dimension; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dataset_type_dimension
    ADD CONSTRAINT pk_dataset_type_dimension PRIMARY KEY (dataset_type_id, domain_id, dimension_id);


--
-- TOC entry 3549 (class 2606 OID 987623)
-- Name: pk_dataset_type_domain; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dataset_type_domain
    ADD CONSTRAINT pk_dataset_type_domain PRIMARY KEY (dataset_type_id, domain_id);


--
-- TOC entry 3554 (class 2606 OID 987625)
-- Name: pk_dataset_type_measurement_type; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dataset_type_measurement_type
    ADD CONSTRAINT pk_dataset_type_measurement_type PRIMARY KEY (dataset_type_id, measurement_metatype_id, measurement_type_id);


--
-- TOC entry 3558 (class 2606 OID 987627)
-- Name: pk_datatype; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY datatype
    ADD CONSTRAINT pk_datatype PRIMARY KEY (datatype_id);


--
-- TOC entry 3562 (class 2606 OID 987629)
-- Name: pk_dimension; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dimension
    ADD CONSTRAINT pk_dimension PRIMARY KEY (dimension_id);


--
-- TOC entry 3570 (class 2606 OID 987631)
-- Name: pk_dimension_domain; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dimension_domain
    ADD CONSTRAINT pk_dimension_domain PRIMARY KEY (domain_id, dimension_id);


--
-- TOC entry 3572 (class 2606 OID 987633)
-- Name: pk_domain; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY domain
    ADD CONSTRAINT pk_domain PRIMARY KEY (domain_id);


--
-- TOC entry 3576 (class 2606 OID 987635)
-- Name: pk_indexing_type; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY indexing_type
    ADD CONSTRAINT pk_indexing_type PRIMARY KEY (indexing_type_id);


--
-- TOC entry 3609 (class 2606 OID 987637)
-- Name: pk_instrument; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY instrument
    ADD CONSTRAINT pk_instrument PRIMARY KEY (instrument_type_id, instrument_id);


--
-- TOC entry 3613 (class 2606 OID 987639)
-- Name: pk_instrument_type; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY instrument_type
    ADD CONSTRAINT pk_instrument_type PRIMARY KEY (instrument_type_id);


--
-- TOC entry 3617 (class 2606 OID 987641)
-- Name: pk_measurement_metatype; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY measurement_metatype
    ADD CONSTRAINT pk_measurement_metatype PRIMARY KEY (measurement_metatype_id);


--
-- TOC entry 3622 (class 2606 OID 987643)
-- Name: pk_measurement_type; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY measurement_type
    ADD CONSTRAINT pk_measurement_type PRIMARY KEY (measurement_metatype_id, measurement_type_id);


--
-- TOC entry 3626 (class 2606 OID 987645)
-- Name: pk_ndarray; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY ndarray
    ADD CONSTRAINT pk_ndarray PRIMARY KEY (ndarray_type_id, ndarray_id, ndarray_version);


--
-- TOC entry 3632 (class 2606 OID 987647)
-- Name: pk_ndarray_dataset; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY ndarray_dataset
    ADD CONSTRAINT pk_ndarray_dataset PRIMARY KEY (ndarray_type_id, ndarray_id, ndarray_version, dataset_type_id, dataset_id);


--
-- TOC entry 3636 (class 2606 OID 987649)
-- Name: pk_ndarray_dimension; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY ndarray_dimension
    ADD CONSTRAINT pk_ndarray_dimension PRIMARY KEY (ndarray_type_id, ndarray_id, ndarray_version, domain_id, dimension_id);


--
-- TOC entry 3638 (class 2606 OID 987651)
-- Name: pk_ndarray_footprint; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY ndarray_footprint
    ADD CONSTRAINT pk_ndarray_footprint PRIMARY KEY (ndarray_type_id, ndarray_footprint_id);


--
-- TOC entry 3642 (class 2606 OID 987653)
-- Name: pk_ndarray_footprint_dimension; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY ndarray_footprint_dimension
    ADD CONSTRAINT pk_ndarray_footprint_dimension PRIMARY KEY (ndarray_type_id, ndarray_footprint_id, domain_id, dimension_id);


--
-- TOC entry 3580 (class 2606 OID 987655)
-- Name: pk_ndarray_type; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY ndarray_type
    ADD CONSTRAINT pk_ndarray_type PRIMARY KEY (ndarray_type_id);


--
-- TOC entry 3587 (class 2606 OID 987657)
-- Name: pk_ndarray_type_dimension; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY ndarray_type_dimension
    ADD CONSTRAINT pk_ndarray_type_dimension PRIMARY KEY (ndarray_type_id, domain_id, dimension_id);


--
-- TOC entry 3601 (class 2606 OID 987659)
-- Name: pk_ndarray_type_dimension_property; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY ndarray_type_dimension_property
    ADD CONSTRAINT pk_ndarray_type_dimension_property PRIMARY KEY (ndarray_type_id, domain_id, dimension_id, property_id);


--
-- TOC entry 3647 (class 2606 OID 987661)
-- Name: pk_ndarray_type_measurement_type; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY ndarray_type_measurement_type
    ADD CONSTRAINT pk_ndarray_type_measurement_type PRIMARY KEY (ndarray_type_id, measurement_metatype_id, measurement_type_id);


--
-- TOC entry 3653 (class 2606 OID 987663)
-- Name: pk_observation; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY observation
    ADD CONSTRAINT pk_observation PRIMARY KEY (observation_type_id, observation_id);


--
-- TOC entry 3655 (class 2606 OID 987665)
-- Name: pk_observation_type; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY observation_type
    ADD CONSTRAINT pk_observation_type PRIMARY KEY (observation_type_id);


--
-- TOC entry 3660 (class 2606 OID 987667)
-- Name: pk_platform; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY platform
    ADD CONSTRAINT pk_platform PRIMARY KEY (platform_type_id, platform_id);


--
-- TOC entry 3664 (class 2606 OID 987669)
-- Name: pk_platform_type; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY platform_type
    ADD CONSTRAINT pk_platform_type PRIMARY KEY (platform_type_id);


--
-- TOC entry 3591 (class 2606 OID 987671)
-- Name: pk_reference_system; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY reference_system
    ADD CONSTRAINT pk_reference_system PRIMARY KEY (reference_system_id);


--
-- TOC entry 3597 (class 2606 OID 987673)
-- Name: pk_reference_system_indexing; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY reference_system_indexing
    ADD CONSTRAINT pk_reference_system_indexing PRIMARY KEY (reference_system_id, array_index);


--
-- TOC entry 3603 (class 2606 OID 987675)
-- Name: property_pkey; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY property
    ADD CONSTRAINT property_pkey PRIMARY KEY (property_id);


--
-- TOC entry 3540 (class 2606 OID 987677)
-- Name: uq_dataset_type_dataset_type_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dataset_type
    ADD CONSTRAINT uq_dataset_type_dataset_type_name UNIQUE (dataset_type_name);


--
-- TOC entry 3556 (class 2606 OID 987679)
-- Name: uq_dataset_type_measurement_type_ndarray_type_id_measurement_ty; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dataset_type_measurement_type
    ADD CONSTRAINT uq_dataset_type_measurement_type_ndarray_type_id_measurement_ty UNIQUE (dataset_type_id, measurement_type_index);


--
-- TOC entry 3560 (class 2606 OID 987681)
-- Name: uq_datatype_datatype_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY datatype
    ADD CONSTRAINT uq_datatype_datatype_name UNIQUE (datatype_name);


--
-- TOC entry 3564 (class 2606 OID 987683)
-- Name: uq_dimension_dimension_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dimension
    ADD CONSTRAINT uq_dimension_dimension_name UNIQUE (dimension_name);


--
-- TOC entry 3566 (class 2606 OID 987685)
-- Name: uq_dimension_dimension_tag; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dimension
    ADD CONSTRAINT uq_dimension_dimension_tag UNIQUE (dimension_tag);


--
-- TOC entry 3574 (class 2606 OID 987687)
-- Name: uq_domain_domain_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY domain
    ADD CONSTRAINT uq_domain_domain_name UNIQUE (domain_name);


--
-- TOC entry 3578 (class 2606 OID 987689)
-- Name: uq_indexing_type_indexing_type_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY indexing_type
    ADD CONSTRAINT uq_indexing_type_indexing_type_name UNIQUE (indexing_type_name);


--
-- TOC entry 3611 (class 2606 OID 987691)
-- Name: uq_instrument_instrument_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY instrument
    ADD CONSTRAINT uq_instrument_instrument_name UNIQUE (instrument_name);


--
-- TOC entry 3615 (class 2606 OID 987693)
-- Name: uq_instrument_type_instrument_type_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY instrument_type
    ADD CONSTRAINT uq_instrument_type_instrument_type_name UNIQUE (instrument_type_name);


--
-- TOC entry 3619 (class 2606 OID 987695)
-- Name: uq_measurement_metatype_measurement_metatype_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY measurement_metatype
    ADD CONSTRAINT uq_measurement_metatype_measurement_metatype_name UNIQUE (measurement_metatype_name);


--
-- TOC entry 3628 (class 2606 OID 987697)
-- Name: uq_ndarray_ndarray_location; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY ndarray
    ADD CONSTRAINT uq_ndarray_ndarray_location UNIQUE (ndarray_location);


--
-- TOC entry 3589 (class 2606 OID 987699)
-- Name: uq_ndarray_type_dimension_ndarray_type_dimension; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY ndarray_type_dimension
    ADD CONSTRAINT uq_ndarray_type_dimension_ndarray_type_dimension UNIQUE (ndarray_type_id, dimension_id);


--
-- TOC entry 3942 (class 0 OID 0)
-- Dependencies: 3589
-- Name: CONSTRAINT uq_ndarray_type_dimension_ndarray_type_dimension ON ndarray_type_dimension; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON CONSTRAINT uq_ndarray_type_dimension_ndarray_type_dimension ON ndarray_type_dimension IS 'Unique constraint to ensure each dimension is only represented once in each ndarray_type';


--
-- TOC entry 3649 (class 2606 OID 987701)
-- Name: uq_ndarray_type_measurement_type_ndarray_type_id_measurement_ty; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY ndarray_type_measurement_type
    ADD CONSTRAINT uq_ndarray_type_measurement_type_ndarray_type_id_measurement_ty UNIQUE (ndarray_type_id, measurement_type_index);


--
-- TOC entry 3582 (class 2606 OID 987703)
-- Name: uq_ndarray_type_ndarray_type_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY ndarray_type
    ADD CONSTRAINT uq_ndarray_type_ndarray_type_name UNIQUE (ndarray_type_name);


--
-- TOC entry 3657 (class 2606 OID 987705)
-- Name: uq_observation_type_observation_type_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY observation_type
    ADD CONSTRAINT uq_observation_type_observation_type_name UNIQUE (observation_type_name);


--
-- TOC entry 3662 (class 2606 OID 987707)
-- Name: uq_platform_platform_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY platform
    ADD CONSTRAINT uq_platform_platform_name UNIQUE (platform_name);


--
-- TOC entry 3666 (class 2606 OID 987709)
-- Name: uq_platform_type_platform_type_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY platform_type
    ADD CONSTRAINT uq_platform_type_platform_type_name UNIQUE (platform_type_name);


--
-- TOC entry 3605 (class 2606 OID 987711)
-- Name: uq_property_property_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY property
    ADD CONSTRAINT uq_property_property_name UNIQUE (property_name);


--
-- TOC entry 3593 (class 2606 OID 987713)
-- Name: uq_reference_system_reference_system_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY reference_system
    ADD CONSTRAINT uq_reference_system_reference_system_name UNIQUE (reference_system_name);


SET search_path = earth_observation, pg_catalog;

--
-- TOC entry 3523 (class 1259 OID 987714)
-- Name: fki_spectral_parameters_measurement_type; Type: INDEX; Schema: earth_observation; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_spectral_parameters_measurement_type ON spectral_parameters USING btree (measurement_metatype_id, measurement_type_id);


SET search_path = public, pg_catalog;

--
-- TOC entry 3526 (class 1259 OID 987715)
-- Name: fki_dataset_dataset_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_dataset_type ON dataset USING btree (dataset_type_id);


--
-- TOC entry 3530 (class 1259 OID 987716)
-- Name: fki_dataset_dimension_dataset; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_dimension_dataset ON dataset_dimension USING btree (dataset_type_id, dataset_id);


--
-- TOC entry 3531 (class 1259 OID 987717)
-- Name: fki_dataset_dimension_dataset_type_dimension; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_dimension_dataset_type_dimension ON dataset_dimension USING btree (dataset_type_id, domain_id, dimension_id);


--
-- TOC entry 3534 (class 1259 OID 987718)
-- Name: fki_dataset_metadata_dataset; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_metadata_dataset ON dataset_metadata USING btree (dataset_type_id, dataset_id);


--
-- TOC entry 3527 (class 1259 OID 987719)
-- Name: fki_dataset_observation; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_observation ON dataset USING btree (observation_type_id, observation_id);


--
-- TOC entry 3541 (class 1259 OID 987720)
-- Name: fki_dataset_type_dimension_dataset_type_domain; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_type_dimension_dataset_type_domain ON dataset_type_dimension USING btree (dataset_type_id, domain_id);


--
-- TOC entry 3542 (class 1259 OID 987721)
-- Name: fki_dataset_type_dimension_dimension_domain; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_type_dimension_dimension_domain ON dataset_type_dimension USING btree (domain_id, dimension_id);


--
-- TOC entry 3545 (class 1259 OID 987722)
-- Name: fki_dataset_type_domain_dataset_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_type_domain_dataset_type ON dataset_type_domain USING btree (dataset_type_id);


--
-- TOC entry 3546 (class 1259 OID 987723)
-- Name: fki_dataset_type_domain_domain; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_type_domain_domain ON dataset_type_domain USING btree (domain_id);


--
-- TOC entry 3547 (class 1259 OID 987724)
-- Name: fki_dataset_type_domain_reference_system; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_type_domain_reference_system ON dataset_type_domain USING btree (reference_system_id);


--
-- TOC entry 3550 (class 1259 OID 987725)
-- Name: fki_dataset_type_measurement_metatype_datatype; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_type_measurement_metatype_datatype ON dataset_type_measurement_type USING btree (datatype_id);


--
-- TOC entry 3551 (class 1259 OID 987726)
-- Name: fki_dataset_type_measurement_type_dataset_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_type_measurement_type_dataset_type ON dataset_type_measurement_type USING btree (dataset_type_id);


--
-- TOC entry 3552 (class 1259 OID 987727)
-- Name: fki_dataset_type_measurement_type_measurement_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_type_measurement_type_measurement_type ON dataset_type_measurement_type USING btree (measurement_metatype_id, measurement_type_id);


--
-- TOC entry 3567 (class 1259 OID 987728)
-- Name: fki_dimension_domain_dimension; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dimension_domain_dimension ON dimension_domain USING btree (dimension_id);


--
-- TOC entry 3568 (class 1259 OID 987729)
-- Name: fki_dimension_domain_domain; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dimension_domain_domain ON dimension_domain USING btree (domain_id);


--
-- TOC entry 3606 (class 1259 OID 987730)
-- Name: fki_instrument_instrument_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_instrument_instrument_type ON instrument USING btree (instrument_type_id);


--
-- TOC entry 3607 (class 1259 OID 987731)
-- Name: fki_instrument_platform; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_instrument_platform ON instrument USING btree (platform_type_id, platform_id);


--
-- TOC entry 3620 (class 1259 OID 987732)
-- Name: fki_measurement_type_measurement_metatype; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_measurement_type_measurement_metatype ON measurement_type USING btree (measurement_metatype_id);


--
-- TOC entry 3629 (class 1259 OID 987733)
-- Name: fki_ndarray_dataset_dataset; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_ndarray_dataset_dataset ON ndarray_dataset USING btree (dataset_type_id, dataset_id);


--
-- TOC entry 3630 (class 1259 OID 987734)
-- Name: fki_ndarray_dataset_ndarray; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_ndarray_dataset_ndarray ON ndarray_dataset USING btree (ndarray_type_id, ndarray_id, ndarray_version);


--
-- TOC entry 3633 (class 1259 OID 987735)
-- Name: fki_ndarray_dimension_ndarray; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_ndarray_dimension_ndarray ON ndarray_dimension USING btree (ndarray_type_id, ndarray_id, ndarray_version);


--
-- TOC entry 3634 (class 1259 OID 987736)
-- Name: fki_ndarray_dimension_ndarray_type_dimension; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_ndarray_dimension_ndarray_type_dimension ON ndarray_dimension USING btree (ndarray_type_id, domain_id, dimension_id);


--
-- TOC entry 3639 (class 1259 OID 987737)
-- Name: fki_ndarray_footprint_dimension_ndarray; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_ndarray_footprint_dimension_ndarray ON ndarray_footprint_dimension USING btree (ndarray_type_id, ndarray_footprint_id);


--
-- TOC entry 3640 (class 1259 OID 987738)
-- Name: fki_ndarray_footprint_dimension_ndarray_type_dimension; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_ndarray_footprint_dimension_ndarray_type_dimension ON ndarray_footprint_dimension USING btree (ndarray_type_id, domain_id, dimension_id);


--
-- TOC entry 3623 (class 1259 OID 987739)
-- Name: fki_ndarray_footprint_ndarray_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_ndarray_footprint_ndarray_type ON ndarray USING btree (ndarray_type_id);


--
-- TOC entry 3624 (class 1259 OID 987740)
-- Name: fki_ndarray_ndarray_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_ndarray_ndarray_type ON ndarray USING btree (ndarray_type_id, ndarray_type_id);


--
-- TOC entry 3598 (class 1259 OID 987741)
-- Name: fki_ndarray_type_dimension_attribute_ndarray_type_dimension; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_ndarray_type_dimension_attribute_ndarray_type_dimension ON ndarray_type_dimension_property USING btree (ndarray_type_id, domain_id, dimension_id);


--
-- TOC entry 3599 (class 1259 OID 987742)
-- Name: fki_ndarray_type_dimension_attribute_property; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_ndarray_type_dimension_attribute_property ON ndarray_type_dimension_property USING btree (property_id);


--
-- TOC entry 3583 (class 1259 OID 987743)
-- Name: fki_ndarray_type_dimension_dimension_domain; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_ndarray_type_dimension_dimension_domain ON ndarray_type_dimension USING btree (domain_id, dimension_id);


--
-- TOC entry 3584 (class 1259 OID 987744)
-- Name: fki_ndarray_type_dimension_indexing_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_ndarray_type_dimension_indexing_type ON ndarray_type_dimension USING btree (indexing_type_id);


--
-- TOC entry 3585 (class 1259 OID 987745)
-- Name: fki_ndarray_type_dimension_ndarray_type_id_fkey; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_ndarray_type_dimension_ndarray_type_id_fkey ON ndarray_type_dimension USING btree (ndarray_type_id, domain_id);


--
-- TOC entry 3643 (class 1259 OID 987746)
-- Name: fki_ndarray_type_masurement_type_datatype; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_ndarray_type_masurement_type_datatype ON ndarray_type_measurement_type USING btree (datatype_id);


--
-- TOC entry 3644 (class 1259 OID 987747)
-- Name: fki_ndarray_type_measurement_type_measurement_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_ndarray_type_measurement_type_measurement_type ON ndarray_type_measurement_type USING btree (measurement_metatype_id, measurement_type_id);


--
-- TOC entry 3645 (class 1259 OID 987748)
-- Name: fki_ndarray_type_measurement_type_ndarray_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_ndarray_type_measurement_type_ndarray_type ON ndarray_type_measurement_type USING btree (ndarray_type_id);


--
-- TOC entry 3650 (class 1259 OID 987749)
-- Name: fki_observation_instrument; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_observation_instrument ON observation USING btree (instrument_type_id, instrument_id);


--
-- TOC entry 3651 (class 1259 OID 987750)
-- Name: fki_observation_observation_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_observation_observation_type ON observation USING btree (observation_type_id);


--
-- TOC entry 3658 (class 1259 OID 987751)
-- Name: fki_platform_platform_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_platform_platform_type ON platform USING btree (platform_type_id);


--
-- TOC entry 3594 (class 1259 OID 987752)
-- Name: fki_reference_system_indexing_measurement_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_reference_system_indexing_measurement_type ON reference_system_indexing USING btree (measurement_metatype_id, measurement_type_id);


--
-- TOC entry 3595 (class 1259 OID 987753)
-- Name: fki_reference_system_indexing_reference_system; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_reference_system_indexing_reference_system ON reference_system_indexing USING btree (reference_system_id);


SET search_path = earth_observation, pg_catalog;

--
-- TOC entry 3667 (class 2606 OID 987754)
-- Name: fk_spectral_parameters_measurement_metatype; Type: FK CONSTRAINT; Schema: earth_observation; Owner: cube_admin
--

ALTER TABLE ONLY spectral_parameters
    ADD CONSTRAINT fk_spectral_parameters_measurement_metatype FOREIGN KEY (measurement_metatype_id, measurement_type_id) REFERENCES public.measurement_type(measurement_metatype_id, measurement_type_id);


SET search_path = public, pg_catalog;

--
-- TOC entry 3668 (class 2606 OID 987759)
-- Name: fk_dataset_dataset_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset
    ADD CONSTRAINT fk_dataset_dataset_type FOREIGN KEY (dataset_type_id) REFERENCES dataset_type(dataset_type_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3670 (class 2606 OID 987764)
-- Name: fk_dataset_dimension_dataset; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset_dimension
    ADD CONSTRAINT fk_dataset_dimension_dataset FOREIGN KEY (dataset_type_id, dataset_id) REFERENCES dataset(dataset_type_id, dataset_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3671 (class 2606 OID 987769)
-- Name: fk_dataset_dimension_dataset_type_dimension; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset_dimension
    ADD CONSTRAINT fk_dataset_dimension_dataset_type_dimension FOREIGN KEY (dataset_type_id, domain_id, dimension_id) REFERENCES dataset_type_dimension(dataset_type_id, domain_id, dimension_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3672 (class 2606 OID 987774)
-- Name: fk_dataset_metadata_dataset; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset_metadata
    ADD CONSTRAINT fk_dataset_metadata_dataset FOREIGN KEY (dataset_type_id, dataset_id) REFERENCES dataset(dataset_type_id, dataset_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3669 (class 2606 OID 987779)
-- Name: fk_dataset_observation; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset
    ADD CONSTRAINT fk_dataset_observation FOREIGN KEY (observation_type_id, observation_id) REFERENCES observation(observation_type_id, observation_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3673 (class 2606 OID 987784)
-- Name: fk_dataset_type_dimension_dataset_type_domain; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset_type_dimension
    ADD CONSTRAINT fk_dataset_type_dimension_dataset_type_domain FOREIGN KEY (dataset_type_id, domain_id) REFERENCES dataset_type_domain(dataset_type_id, domain_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3674 (class 2606 OID 987789)
-- Name: fk_dataset_type_dimension_dimension_domain; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset_type_dimension
    ADD CONSTRAINT fk_dataset_type_dimension_dimension_domain FOREIGN KEY (domain_id, dimension_id) REFERENCES dimension_domain(domain_id, dimension_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3675 (class 2606 OID 987794)
-- Name: fk_dataset_type_domain_dataset_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset_type_domain
    ADD CONSTRAINT fk_dataset_type_domain_dataset_type FOREIGN KEY (dataset_type_id) REFERENCES dataset_type(dataset_type_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3676 (class 2606 OID 987799)
-- Name: fk_dataset_type_domain_domain; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset_type_domain
    ADD CONSTRAINT fk_dataset_type_domain_domain FOREIGN KEY (domain_id) REFERENCES domain(domain_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3677 (class 2606 OID 987804)
-- Name: fk_dataset_type_domain_reference_system; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset_type_domain
    ADD CONSTRAINT fk_dataset_type_domain_reference_system FOREIGN KEY (reference_system_id) REFERENCES reference_system(reference_system_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3678 (class 2606 OID 987809)
-- Name: fk_dataset_type_measurement_type_dataset_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset_type_measurement_type
    ADD CONSTRAINT fk_dataset_type_measurement_type_dataset_type FOREIGN KEY (dataset_type_id) REFERENCES dataset_type(dataset_type_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3679 (class 2606 OID 987814)
-- Name: fk_dataset_type_measurement_type_datatype; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset_type_measurement_type
    ADD CONSTRAINT fk_dataset_type_measurement_type_datatype FOREIGN KEY (datatype_id) REFERENCES datatype(datatype_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3680 (class 2606 OID 987819)
-- Name: fk_dataset_type_measurement_type_measurement_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset_type_measurement_type
    ADD CONSTRAINT fk_dataset_type_measurement_type_measurement_type FOREIGN KEY (measurement_metatype_id, measurement_type_id) REFERENCES measurement_type(measurement_metatype_id, measurement_type_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3681 (class 2606 OID 987824)
-- Name: fk_dimension_domain_dimension; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dimension_domain
    ADD CONSTRAINT fk_dimension_domain_dimension FOREIGN KEY (dimension_id) REFERENCES dimension(dimension_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3682 (class 2606 OID 987829)
-- Name: fk_dimension_domain_domain; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dimension_domain
    ADD CONSTRAINT fk_dimension_domain_domain FOREIGN KEY (domain_id) REFERENCES domain(domain_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3691 (class 2606 OID 987834)
-- Name: fk_instrument_instrument_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY instrument
    ADD CONSTRAINT fk_instrument_instrument_type FOREIGN KEY (instrument_type_id) REFERENCES instrument_type(instrument_type_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3692 (class 2606 OID 987839)
-- Name: fk_instrument_platform; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY instrument
    ADD CONSTRAINT fk_instrument_platform FOREIGN KEY (platform_type_id, platform_id) REFERENCES platform(platform_type_id, platform_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3693 (class 2606 OID 987844)
-- Name: fk_measurement_type_measurement_metatype; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY measurement_type
    ADD CONSTRAINT fk_measurement_type_measurement_metatype FOREIGN KEY (measurement_metatype_id) REFERENCES measurement_metatype(measurement_metatype_id);


--
-- TOC entry 3695 (class 2606 OID 987849)
-- Name: fk_ndarray_dataset_dataset; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY ndarray_dataset
    ADD CONSTRAINT fk_ndarray_dataset_dataset FOREIGN KEY (dataset_type_id, dataset_id) REFERENCES dataset(dataset_type_id, dataset_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3696 (class 2606 OID 987854)
-- Name: fk_ndarray_dataset_ndarray; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY ndarray_dataset
    ADD CONSTRAINT fk_ndarray_dataset_ndarray FOREIGN KEY (ndarray_type_id, ndarray_id, ndarray_version) REFERENCES ndarray(ndarray_type_id, ndarray_id, ndarray_version) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3697 (class 2606 OID 987859)
-- Name: fk_ndarray_dimension_ndarray; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY ndarray_dimension
    ADD CONSTRAINT fk_ndarray_dimension_ndarray FOREIGN KEY (ndarray_type_id, ndarray_id, ndarray_version) REFERENCES ndarray(ndarray_type_id, ndarray_id, ndarray_version) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3698 (class 2606 OID 987864)
-- Name: fk_ndarray_dimension_ndarray_type_dimension; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY ndarray_dimension
    ADD CONSTRAINT fk_ndarray_dimension_ndarray_type_dimension FOREIGN KEY (ndarray_type_id, domain_id, dimension_id) REFERENCES ndarray_type_dimension(ndarray_type_id, domain_id, dimension_id);


--
-- TOC entry 3700 (class 2606 OID 987869)
-- Name: fk_ndarray_footprint_dimension_ndarray_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY ndarray_footprint_dimension
    ADD CONSTRAINT fk_ndarray_footprint_dimension_ndarray_type FOREIGN KEY (ndarray_type_id, ndarray_footprint_id) REFERENCES ndarray_footprint(ndarray_type_id, ndarray_footprint_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3701 (class 2606 OID 987874)
-- Name: fk_ndarray_footprint_dimension_ndarray_type_dimension; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY ndarray_footprint_dimension
    ADD CONSTRAINT fk_ndarray_footprint_dimension_ndarray_type_dimension FOREIGN KEY (ndarray_type_id, domain_id, dimension_id) REFERENCES ndarray_type_dimension(ndarray_type_id, domain_id, dimension_id);


--
-- TOC entry 3699 (class 2606 OID 987879)
-- Name: fk_ndarray_footprint_ndarray_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY ndarray_footprint
    ADD CONSTRAINT fk_ndarray_footprint_ndarray_type FOREIGN KEY (ndarray_type_id) REFERENCES ndarray_type(ndarray_type_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3694 (class 2606 OID 987884)
-- Name: fk_ndarray_ndarray_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY ndarray
    ADD CONSTRAINT fk_ndarray_ndarray_type FOREIGN KEY (ndarray_type_id) REFERENCES ndarray_type(ndarray_type_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3689 (class 2606 OID 987889)
-- Name: fk_ndarray_type_dimension_attribute_ndarray_type_dimension; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY ndarray_type_dimension_property
    ADD CONSTRAINT fk_ndarray_type_dimension_attribute_ndarray_type_dimension FOREIGN KEY (ndarray_type_id, domain_id, dimension_id) REFERENCES ndarray_type_dimension(ndarray_type_id, domain_id, dimension_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3683 (class 2606 OID 987894)
-- Name: fk_ndarray_type_dimension_dimension_domain; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY ndarray_type_dimension
    ADD CONSTRAINT fk_ndarray_type_dimension_dimension_domain FOREIGN KEY (domain_id, dimension_id) REFERENCES dimension_domain(domain_id, dimension_id) ON UPDATE CASCADE;


--
-- TOC entry 3685 (class 2606 OID 994219)
-- Name: fk_ndarray_type_dimension_indexing_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY ndarray_type_dimension
    ADD CONSTRAINT fk_ndarray_type_dimension_indexing_type FOREIGN KEY (indexing_type_id) REFERENCES indexing_type(indexing_type_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3686 (class 2606 OID 994224)
-- Name: fk_ndarray_type_dimension_ndarray_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY ndarray_type_dimension
    ADD CONSTRAINT fk_ndarray_type_dimension_ndarray_type FOREIGN KEY (ndarray_type_id) REFERENCES ndarray_type(ndarray_type_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3690 (class 2606 OID 987904)
-- Name: fk_ndarray_type_dimension_property_property; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY ndarray_type_dimension_property
    ADD CONSTRAINT fk_ndarray_type_dimension_property_property FOREIGN KEY (property_id) REFERENCES property(property_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3684 (class 2606 OID 987909)
-- Name: fk_ndarray_type_dimension_reference_system; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY ndarray_type_dimension
    ADD CONSTRAINT fk_ndarray_type_dimension_reference_system FOREIGN KEY (reference_system_id) REFERENCES reference_system(reference_system_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3702 (class 2606 OID 987914)
-- Name: fk_ndarray_type_measurement_type_datatype; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY ndarray_type_measurement_type
    ADD CONSTRAINT fk_ndarray_type_measurement_type_datatype FOREIGN KEY (datatype_id) REFERENCES datatype(datatype_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3703 (class 2606 OID 987919)
-- Name: fk_ndarray_type_measurement_type_measurement_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY ndarray_type_measurement_type
    ADD CONSTRAINT fk_ndarray_type_measurement_type_measurement_type FOREIGN KEY (measurement_metatype_id, measurement_type_id) REFERENCES measurement_type(measurement_metatype_id, measurement_type_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3704 (class 2606 OID 987924)
-- Name: fk_ndarray_type_measurement_type_ndarray_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY ndarray_type_measurement_type
    ADD CONSTRAINT fk_ndarray_type_measurement_type_ndarray_type FOREIGN KEY (ndarray_type_id) REFERENCES ndarray_type(ndarray_type_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3705 (class 2606 OID 987929)
-- Name: fk_observation_instrument; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY observation
    ADD CONSTRAINT fk_observation_instrument FOREIGN KEY (instrument_type_id, instrument_id) REFERENCES instrument(instrument_type_id, instrument_id);


--
-- TOC entry 3706 (class 2606 OID 987934)
-- Name: fk_observation_observation_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY observation
    ADD CONSTRAINT fk_observation_observation_type FOREIGN KEY (observation_type_id) REFERENCES observation_type(observation_type_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3707 (class 2606 OID 987939)
-- Name: fk_platform_platform_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY platform
    ADD CONSTRAINT fk_platform_platform_type FOREIGN KEY (platform_type_id) REFERENCES platform_type(platform_type_id);


--
-- TOC entry 3687 (class 2606 OID 987944)
-- Name: fk_reference_system_indexing_measurement_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY reference_system_indexing
    ADD CONSTRAINT fk_reference_system_indexing_measurement_type FOREIGN KEY (measurement_metatype_id, measurement_type_id) REFERENCES measurement_type(measurement_metatype_id, measurement_type_id);


--
-- TOC entry 3688 (class 2606 OID 987949)
-- Name: fk_reference_system_indexing_reference_system; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY reference_system_indexing
    ADD CONSTRAINT fk_reference_system_indexing_reference_system FOREIGN KEY (reference_system_id) REFERENCES reference_system(reference_system_id);


--
-- TOC entry 3841 (class 0 OID 987539)
-- Dependencies: 211 3862
-- Name: ndarray_type_dimension_view; Type: MATERIALIZED VIEW DATA; Schema: public; Owner: cube_admin
--

REFRESH MATERIALIZED VIEW ndarray_type_dimension_view;


--
-- TOC entry 3843 (class 0 OID 987550)
-- Dependencies: 213 3841 3862
-- Name: dimension_indices_view; Type: MATERIALIZED VIEW DATA; Schema: public; Owner: cube_admin
--

REFRESH MATERIALIZED VIEW dimension_indices_view;


--
-- TOC entry 3846 (class 0 OID 987560)
-- Dependencies: 216 3841 3862
-- Name: dimension_properties_view; Type: MATERIALIZED VIEW DATA; Schema: public; Owner: cube_admin
--

REFRESH MATERIALIZED VIEW dimension_properties_view;


--
-- TOC entry 3868 (class 0 OID 0)
-- Dependencies: 8
-- Name: public; Type: ACL; Schema: -; Owner: postgres
--

REVOKE ALL ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM postgres;
GRANT ALL ON SCHEMA public TO postgres;
GRANT ALL ON SCHEMA public TO cube_admin;
GRANT ALL ON SCHEMA public TO PUBLIC;


--
-- TOC entry 3870 (class 0 OID 0)
-- Dependencies: 9
-- Name: ztmp; Type: ACL; Schema: -; Owner: postgres
--

REVOKE ALL ON SCHEMA ztmp FROM PUBLIC;
REVOKE ALL ON SCHEMA ztmp FROM postgres;
GRANT ALL ON SCHEMA ztmp TO postgres;
GRANT ALL ON SCHEMA ztmp TO PUBLIC;


SET search_path = earth_observation, pg_catalog;

--
-- TOC entry 3876 (class 0 OID 0)
-- Dependencies: 195
-- Name: spectral_parameters; Type: ACL; Schema: earth_observation; Owner: cube_admin
--

REVOKE ALL ON TABLE spectral_parameters FROM PUBLIC;
REVOKE ALL ON TABLE spectral_parameters FROM cube_admin;
GRANT ALL ON TABLE spectral_parameters TO cube_admin;
GRANT ALL ON TABLE spectral_parameters TO cube_admin_group;
GRANT SELECT ON TABLE spectral_parameters TO cube_user_group;


SET search_path = public, pg_catalog;

--
-- TOC entry 3878 (class 0 OID 0)
-- Dependencies: 196
-- Name: dataset; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE dataset FROM PUBLIC;
REVOKE ALL ON TABLE dataset FROM cube_admin;
GRANT ALL ON TABLE dataset TO cube_admin;
GRANT ALL ON TABLE dataset TO cube_admin_group;
GRANT SELECT ON TABLE dataset TO cube_user_group;


--
-- TOC entry 3880 (class 0 OID 0)
-- Dependencies: 197
-- Name: dataset_dimension; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE dataset_dimension FROM PUBLIC;
REVOKE ALL ON TABLE dataset_dimension FROM cube_admin;
GRANT ALL ON TABLE dataset_dimension TO cube_admin;
GRANT ALL ON TABLE dataset_dimension TO cube_admin_group;
GRANT SELECT ON TABLE dataset_dimension TO cube_user_group;


--
-- TOC entry 3882 (class 0 OID 0)
-- Dependencies: 198
-- Name: dataset_metadata; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE dataset_metadata FROM PUBLIC;
REVOKE ALL ON TABLE dataset_metadata FROM cube_admin;
GRANT ALL ON TABLE dataset_metadata TO cube_admin;
GRANT ALL ON TABLE dataset_metadata TO cube_admin_group;
GRANT SELECT ON TABLE dataset_metadata TO cube_user_group;


--
-- TOC entry 3884 (class 0 OID 0)
-- Dependencies: 199
-- Name: dataset_type; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE dataset_type FROM PUBLIC;
REVOKE ALL ON TABLE dataset_type FROM cube_admin;
GRANT ALL ON TABLE dataset_type TO cube_admin;
GRANT ALL ON TABLE dataset_type TO cube_admin_group;
GRANT SELECT ON TABLE dataset_type TO cube_user_group;


--
-- TOC entry 3886 (class 0 OID 0)
-- Dependencies: 200
-- Name: dataset_type_dimension; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE dataset_type_dimension FROM PUBLIC;
REVOKE ALL ON TABLE dataset_type_dimension FROM cube_admin;
GRANT ALL ON TABLE dataset_type_dimension TO cube_admin;
GRANT ALL ON TABLE dataset_type_dimension TO cube_admin_group;
GRANT SELECT ON TABLE dataset_type_dimension TO cube_user_group;


--
-- TOC entry 3888 (class 0 OID 0)
-- Dependencies: 201
-- Name: dataset_type_domain; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE dataset_type_domain FROM PUBLIC;
REVOKE ALL ON TABLE dataset_type_domain FROM cube_admin;
GRANT ALL ON TABLE dataset_type_domain TO cube_admin;
GRANT ALL ON TABLE dataset_type_domain TO cube_admin_group;
GRANT SELECT ON TABLE dataset_type_domain TO cube_user_group;


--
-- TOC entry 3890 (class 0 OID 0)
-- Dependencies: 202
-- Name: dataset_type_measurement_type; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE dataset_type_measurement_type FROM PUBLIC;
REVOKE ALL ON TABLE dataset_type_measurement_type FROM cube_admin;
GRANT ALL ON TABLE dataset_type_measurement_type TO cube_admin;
GRANT ALL ON TABLE dataset_type_measurement_type TO cube_admin_group;
GRANT SELECT ON TABLE dataset_type_measurement_type TO cube_user_group;


--
-- TOC entry 3892 (class 0 OID 0)
-- Dependencies: 203
-- Name: datatype; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE datatype FROM PUBLIC;
REVOKE ALL ON TABLE datatype FROM cube_admin;
GRANT ALL ON TABLE datatype TO cube_admin;
GRANT ALL ON TABLE datatype TO cube_admin_group;
GRANT SELECT ON TABLE datatype TO cube_user_group;


--
-- TOC entry 3894 (class 0 OID 0)
-- Dependencies: 204
-- Name: dimension; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE dimension FROM PUBLIC;
REVOKE ALL ON TABLE dimension FROM cube_admin;
GRANT ALL ON TABLE dimension TO cube_admin;
GRANT ALL ON TABLE dimension TO cube_admin_group;
GRANT SELECT ON TABLE dimension TO cube_user_group;


--
-- TOC entry 3896 (class 0 OID 0)
-- Dependencies: 205
-- Name: dimension_domain; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE dimension_domain FROM PUBLIC;
REVOKE ALL ON TABLE dimension_domain FROM cube_admin;
GRANT ALL ON TABLE dimension_domain TO cube_admin;
GRANT ALL ON TABLE dimension_domain TO cube_admin_group;
GRANT SELECT ON TABLE dimension_domain TO cube_user_group;


--
-- TOC entry 3898 (class 0 OID 0)
-- Dependencies: 206
-- Name: domain; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE domain FROM PUBLIC;
REVOKE ALL ON TABLE domain FROM cube_admin;
GRANT ALL ON TABLE domain TO cube_admin;
GRANT ALL ON TABLE domain TO cube_admin_group;
GRANT SELECT ON TABLE domain TO cube_user_group;


--
-- TOC entry 3900 (class 0 OID 0)
-- Dependencies: 207
-- Name: indexing_type; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE indexing_type FROM PUBLIC;
REVOKE ALL ON TABLE indexing_type FROM cube_admin;
GRANT ALL ON TABLE indexing_type TO cube_admin;
GRANT ALL ON TABLE indexing_type TO cube_admin_group;
GRANT SELECT ON TABLE indexing_type TO cube_user_group;


--
-- TOC entry 3902 (class 0 OID 0)
-- Dependencies: 208
-- Name: ndarray_type; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE ndarray_type FROM PUBLIC;
REVOKE ALL ON TABLE ndarray_type FROM cube_admin;
GRANT ALL ON TABLE ndarray_type TO cube_admin;
GRANT ALL ON TABLE ndarray_type TO cube_admin_group;
GRANT SELECT ON TABLE ndarray_type TO cube_user_group;


--
-- TOC entry 3904 (class 0 OID 0)
-- Dependencies: 209
-- Name: ndarray_type_dimension; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE ndarray_type_dimension FROM PUBLIC;
REVOKE ALL ON TABLE ndarray_type_dimension FROM cube_admin;
GRANT ALL ON TABLE ndarray_type_dimension TO cube_admin;
GRANT ALL ON TABLE ndarray_type_dimension TO cube_admin_group;
GRANT SELECT ON TABLE ndarray_type_dimension TO cube_user_group;


--
-- TOC entry 3906 (class 0 OID 0)
-- Dependencies: 210
-- Name: reference_system; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE reference_system FROM PUBLIC;
REVOKE ALL ON TABLE reference_system FROM cube_admin;
GRANT ALL ON TABLE reference_system TO cube_admin;
GRANT ALL ON TABLE reference_system TO cube_admin_group;
GRANT SELECT ON TABLE reference_system TO cube_user_group;


--
-- TOC entry 3907 (class 0 OID 0)
-- Dependencies: 211
-- Name: ndarray_type_dimension_view; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE ndarray_type_dimension_view FROM PUBLIC;
REVOKE ALL ON TABLE ndarray_type_dimension_view FROM cube_admin;
GRANT ALL ON TABLE ndarray_type_dimension_view TO cube_admin;
GRANT ALL ON TABLE ndarray_type_dimension_view TO cube_admin_group;
GRANT SELECT ON TABLE ndarray_type_dimension_view TO cube_user_group;


--
-- TOC entry 3909 (class 0 OID 0)
-- Dependencies: 212
-- Name: reference_system_indexing; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE reference_system_indexing FROM PUBLIC;
REVOKE ALL ON TABLE reference_system_indexing FROM cube_admin;
GRANT ALL ON TABLE reference_system_indexing TO cube_admin;
GRANT ALL ON TABLE reference_system_indexing TO cube_admin_group;
GRANT SELECT ON TABLE reference_system_indexing TO cube_user_group;


--
-- TOC entry 3910 (class 0 OID 0)
-- Dependencies: 213
-- Name: dimension_indices_view; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE dimension_indices_view FROM PUBLIC;
REVOKE ALL ON TABLE dimension_indices_view FROM cube_admin;
GRANT ALL ON TABLE dimension_indices_view TO cube_admin;
GRANT ALL ON TABLE dimension_indices_view TO cube_admin_group;
GRANT SELECT ON TABLE dimension_indices_view TO cube_user_group;


--
-- TOC entry 3911 (class 0 OID 0)
-- Dependencies: 214
-- Name: ndarray_type_dimension_property; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE ndarray_type_dimension_property FROM PUBLIC;
REVOKE ALL ON TABLE ndarray_type_dimension_property FROM cube_admin;
GRANT ALL ON TABLE ndarray_type_dimension_property TO cube_admin;
GRANT ALL ON TABLE ndarray_type_dimension_property TO cube_admin_group;
GRANT SELECT ON TABLE ndarray_type_dimension_property TO cube_user_group;


--
-- TOC entry 3912 (class 0 OID 0)
-- Dependencies: 215
-- Name: property; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE property FROM PUBLIC;
REVOKE ALL ON TABLE property FROM cube_admin;
GRANT ALL ON TABLE property TO cube_admin;
GRANT ALL ON TABLE property TO cube_admin_group;
GRANT SELECT ON TABLE property TO cube_user_group;


--
-- TOC entry 3913 (class 0 OID 0)
-- Dependencies: 216
-- Name: dimension_properties_view; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE dimension_properties_view FROM PUBLIC;
REVOKE ALL ON TABLE dimension_properties_view FROM cube_admin;
GRANT ALL ON TABLE dimension_properties_view TO cube_admin;
GRANT ALL ON TABLE dimension_properties_view TO cube_admin_group;
GRANT SELECT ON TABLE dimension_properties_view TO cube_user_group;


--
-- TOC entry 3915 (class 0 OID 0)
-- Dependencies: 217
-- Name: instrument; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE instrument FROM PUBLIC;
REVOKE ALL ON TABLE instrument FROM cube_admin;
GRANT ALL ON TABLE instrument TO cube_admin;
GRANT ALL ON TABLE instrument TO cube_admin_group;
GRANT SELECT ON TABLE instrument TO cube_user_group;


--
-- TOC entry 3917 (class 0 OID 0)
-- Dependencies: 218
-- Name: instrument_type; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE instrument_type FROM PUBLIC;
REVOKE ALL ON TABLE instrument_type FROM cube_admin;
GRANT ALL ON TABLE instrument_type TO cube_admin;
GRANT ALL ON TABLE instrument_type TO cube_admin_group;
GRANT SELECT ON TABLE instrument_type TO cube_user_group;


--
-- TOC entry 3919 (class 0 OID 0)
-- Dependencies: 219
-- Name: measurement_metatype; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE measurement_metatype FROM PUBLIC;
REVOKE ALL ON TABLE measurement_metatype FROM cube_admin;
GRANT ALL ON TABLE measurement_metatype TO cube_admin;
GRANT ALL ON TABLE measurement_metatype TO cube_admin_group;
GRANT SELECT ON TABLE measurement_metatype TO cube_user_group;


--
-- TOC entry 3921 (class 0 OID 0)
-- Dependencies: 220
-- Name: measurement_type; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE measurement_type FROM PUBLIC;
REVOKE ALL ON TABLE measurement_type FROM cube_admin;
GRANT ALL ON TABLE measurement_type TO cube_admin;
GRANT ALL ON TABLE measurement_type TO cube_admin_group;
GRANT SELECT ON TABLE measurement_type TO cube_user_group;


--
-- TOC entry 3923 (class 0 OID 0)
-- Dependencies: 221
-- Name: ndarray; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE ndarray FROM PUBLIC;
REVOKE ALL ON TABLE ndarray FROM cube_admin;
GRANT ALL ON TABLE ndarray TO cube_admin;
GRANT ALL ON TABLE ndarray TO cube_admin_group;
GRANT SELECT ON TABLE ndarray TO cube_user_group;


--
-- TOC entry 3925 (class 0 OID 0)
-- Dependencies: 222
-- Name: ndarray_dataset; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE ndarray_dataset FROM PUBLIC;
REVOKE ALL ON TABLE ndarray_dataset FROM cube_admin;
GRANT ALL ON TABLE ndarray_dataset TO cube_admin;
GRANT ALL ON TABLE ndarray_dataset TO cube_admin_group;
GRANT SELECT ON TABLE ndarray_dataset TO cube_user_group;


--
-- TOC entry 3927 (class 0 OID 0)
-- Dependencies: 223
-- Name: ndarray_dimension; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE ndarray_dimension FROM PUBLIC;
REVOKE ALL ON TABLE ndarray_dimension FROM cube_admin;
GRANT ALL ON TABLE ndarray_dimension TO cube_admin;
GRANT ALL ON TABLE ndarray_dimension TO cube_admin_group;
GRANT SELECT ON TABLE ndarray_dimension TO cube_user_group;


--
-- TOC entry 3929 (class 0 OID 0)
-- Dependencies: 224
-- Name: ndarray_footprint; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE ndarray_footprint FROM PUBLIC;
REVOKE ALL ON TABLE ndarray_footprint FROM cube_admin;
GRANT ALL ON TABLE ndarray_footprint TO cube_admin;
GRANT ALL ON TABLE ndarray_footprint TO cube_admin_group;
GRANT SELECT ON TABLE ndarray_footprint TO cube_user_group;


--
-- TOC entry 3931 (class 0 OID 0)
-- Dependencies: 225
-- Name: ndarray_footprint_dimension; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE ndarray_footprint_dimension FROM PUBLIC;
REVOKE ALL ON TABLE ndarray_footprint_dimension FROM cube_admin;
GRANT ALL ON TABLE ndarray_footprint_dimension TO cube_admin;
GRANT ALL ON TABLE ndarray_footprint_dimension TO cube_admin_group;
GRANT SELECT ON TABLE ndarray_footprint_dimension TO cube_user_group;


--
-- TOC entry 3933 (class 0 OID 0)
-- Dependencies: 226
-- Name: ndarray_type_measurement_type; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE ndarray_type_measurement_type FROM PUBLIC;
REVOKE ALL ON TABLE ndarray_type_measurement_type FROM cube_admin;
GRANT ALL ON TABLE ndarray_type_measurement_type TO cube_admin;
GRANT ALL ON TABLE ndarray_type_measurement_type TO cube_admin_group;
GRANT SELECT ON TABLE ndarray_type_measurement_type TO cube_user_group;


--
-- TOC entry 3935 (class 0 OID 0)
-- Dependencies: 227
-- Name: observation; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE observation FROM PUBLIC;
REVOKE ALL ON TABLE observation FROM cube_admin;
GRANT ALL ON TABLE observation TO cube_admin;
GRANT ALL ON TABLE observation TO cube_admin_group;
GRANT SELECT ON TABLE observation TO cube_user_group;


--
-- TOC entry 3937 (class 0 OID 0)
-- Dependencies: 228
-- Name: observation_type; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE observation_type FROM PUBLIC;
REVOKE ALL ON TABLE observation_type FROM cube_admin;
GRANT ALL ON TABLE observation_type TO cube_admin;
GRANT ALL ON TABLE observation_type TO cube_admin_group;
GRANT SELECT ON TABLE observation_type TO cube_user_group;


--
-- TOC entry 3939 (class 0 OID 0)
-- Dependencies: 229
-- Name: platform; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE platform FROM PUBLIC;
REVOKE ALL ON TABLE platform FROM cube_admin;
GRANT ALL ON TABLE platform TO cube_admin;
GRANT ALL ON TABLE platform TO cube_admin_group;
GRANT SELECT ON TABLE platform TO cube_user_group;


--
-- TOC entry 3941 (class 0 OID 0)
-- Dependencies: 230
-- Name: platform_type; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE platform_type FROM PUBLIC;
REVOKE ALL ON TABLE platform_type FROM cube_admin;
GRANT ALL ON TABLE platform_type TO cube_admin;
GRANT ALL ON TABLE platform_type TO cube_admin_group;
GRANT SELECT ON TABLE platform_type TO cube_user_group;


-- Completed on 2015-03-29 20:59:10 AEDT

--
-- PostgreSQL database dump complete
--

