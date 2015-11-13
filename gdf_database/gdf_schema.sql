--
-- PostgreSQL database dump
--

-- Dumped from database version 9.3.9
-- Dumped by pg_dump version 9.3.1
-- Started on 2015-07-22 17:37:42

SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;

--
-- TOC entry 6 (class 2615 OID 3524008)
-- Name: earth_observation; Type: SCHEMA; Schema: -; Owner: cube_admin
--

CREATE SCHEMA earth_observation;


ALTER SCHEMA earth_observation OWNER TO cube_admin;

--
-- TOC entry 9 (class 2615 OID 3524010)
-- Name: ztmp; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA ztmp;


ALTER SCHEMA ztmp OWNER TO postgres;

--
-- TOC entry 4522 (class 0 OID 0)
-- Dependencies: 9
-- Name: SCHEMA ztmp; Type: COMMENT; Schema: -; Owner: postgres
--

COMMENT ON SCHEMA ztmp IS 'Temporary schema';


SET search_path = public, pg_catalog;

--
-- TOC entry 1718 (class 1247 OID 3524013)
-- Name: attribute_value_type; Type: TYPE; Schema: public; Owner: cube_admin
--

CREATE TYPE attribute_value_type AS (
	attribute_type_id bigint,
	attribute_id bigint,
	attribute_value_id bigint
);


ALTER TYPE public.attribute_value_type OWNER TO cube_admin;

--
-- TOC entry 1721 (class 1247 OID 3524016)
-- Name: category_id_level_type; Type: TYPE; Schema: public; Owner: cube_admin
--

CREATE TYPE category_id_level_type AS (
	category_type_id bigint,
	category_id bigint,
	level integer
);


ALTER TYPE public.category_id_level_type OWNER TO cube_admin;

--
-- TOC entry 1724 (class 1247 OID 3524019)
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
-- TOC entry 176 (class 1259 OID 3524020)
-- Name: spectral_parameters; Type: TABLE; Schema: earth_observation; Owner: cube_admin; Tablespace: 
--

CREATE TABLE spectral_parameters (
    measurement_metatype_id bigint NOT NULL,
    measurement_type_id bigint NOT NULL
);


ALTER TABLE earth_observation.spectral_parameters OWNER TO cube_admin;

--
-- TOC entry 4524 (class 0 OID 0)
-- Dependencies: 176
-- Name: TABLE spectral_parameters; Type: COMMENT; Schema: earth_observation; Owner: cube_admin
--

COMMENT ON TABLE spectral_parameters IS 'Configuration: Spectral band parameters';


SET search_path = public, pg_catalog;

--
-- TOC entry 177 (class 1259 OID 3524023)
-- Name: dataset; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE dataset (
    dataset_type_id bigint NOT NULL,
    dataset_id bigint NOT NULL,
    observation_type_id bigint NOT NULL,
    observation_id bigint NOT NULL,
    dataset_location character varying(254),
    creation_datetime timestamp with time zone,
    dataset_bytes bigint,
    dataset_md5_checksum character(32)
);


ALTER TABLE public.dataset OWNER TO cube_admin;

--
-- TOC entry 4526 (class 0 OID 0)
-- Dependencies: 177
-- Name: TABLE dataset; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE dataset IS 'Data: Source dataset (file) ingested.
An example would be a dataset for a particular NBAR Landsat scene.';


--
-- TOC entry 4527 (class 0 OID 0)
-- Dependencies: 177
-- Name: COLUMN dataset.dataset_type_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN dataset.dataset_type_id IS 'Foreign key to dataset_type. Part of composite primary key for dataset record (other key is dataset_id).';


--
-- TOC entry 4528 (class 0 OID 0)
-- Dependencies: 177
-- Name: COLUMN dataset.dataset_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN dataset.dataset_id IS 'Part of composite primary key for dataset. Other key is dataset_type_id.';


--
-- TOC entry 4529 (class 0 OID 0)
-- Dependencies: 177
-- Name: COLUMN dataset.observation_type_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN dataset.observation_type_id IS 'Part of composite foreign key to observation (other key is observation_id). Also indirect reference to observation_type.';


--
-- TOC entry 4530 (class 0 OID 0)
-- Dependencies: 177
-- Name: COLUMN dataset.observation_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN dataset.observation_id IS 'Part of composite foreign key to observation (other key is observation_type_id).';


--
-- TOC entry 4531 (class 0 OID 0)
-- Dependencies: 177
-- Name: COLUMN dataset.dataset_location; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN dataset.dataset_location IS 'Fully qualified path to source dataset file';


--
-- TOC entry 4532 (class 0 OID 0)
-- Dependencies: 177
-- Name: COLUMN dataset.creation_datetime; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN dataset.creation_datetime IS 'Timestamp for source dataset creation (read from metadata)';


--
-- TOC entry 4533 (class 0 OID 0)
-- Dependencies: 177
-- Name: COLUMN dataset.dataset_bytes; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN dataset.dataset_bytes IS 'Number of bytes in source dataset file';


--
-- TOC entry 4534 (class 0 OID 0)
-- Dependencies: 177
-- Name: COLUMN dataset.dataset_md5_checksum; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN dataset.dataset_md5_checksum IS 'MD5 checksum for source dataset file';


--
-- TOC entry 178 (class 1259 OID 3524026)
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
-- TOC entry 4536 (class 0 OID 0)
-- Dependencies: 178
-- Name: TABLE dataset_dimension; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE dataset_dimension IS 'Data: Dimensional parameters for each source dataset.
Each dataset/dimension will have specific max/min/indexing values showing the range covered by the dataset in that particular dimension.';


--
-- TOC entry 4537 (class 0 OID 0)
-- Dependencies: 178
-- Name: COLUMN dataset_dimension.dataset_type_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN dataset_dimension.dataset_type_id IS 'Part of composite foreign key to dataset (other key is dataset_id). Also indirect reference to dataset_type';


--
-- TOC entry 4538 (class 0 OID 0)
-- Dependencies: 178
-- Name: COLUMN dataset_dimension.dataset_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN dataset_dimension.dataset_id IS 'Part of composite foreign key to dataset (other key is dataset_type_id).';


--
-- TOC entry 4539 (class 0 OID 0)
-- Dependencies: 178
-- Name: COLUMN dataset_dimension.domain_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN dataset_dimension.domain_id IS 'Part of composite foreign key to dataset_type_dimension (other key is dimension_id). Also indirect reference to domain.';


--
-- TOC entry 4540 (class 0 OID 0)
-- Dependencies: 178
-- Name: COLUMN dataset_dimension.dimension_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN dataset_dimension.dimension_id IS 'Part of composite foreign key to dataset_type_dimension (other key is domain_id). Also indirect reference to dimension.';


--
-- TOC entry 4541 (class 0 OID 0)
-- Dependencies: 178
-- Name: COLUMN dataset_dimension.min_value; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN dataset_dimension.min_value IS 'Minimum value in specified dimension for source dataset.';


--
-- TOC entry 4542 (class 0 OID 0)
-- Dependencies: 178
-- Name: COLUMN dataset_dimension.max_value; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN dataset_dimension.max_value IS 'Maximum value in specified dimension for source dataset.';


--
-- TOC entry 4543 (class 0 OID 0)
-- Dependencies: 178
-- Name: COLUMN dataset_dimension.indexing_value; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN dataset_dimension.indexing_value IS 'Value used for indexing in specified dimension for source dataset. Only set for irregular dimensions (e.g. time for EO data).';


--
-- TOC entry 179 (class 1259 OID 3524029)
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
-- TOC entry 180 (class 1259 OID 3524031)
-- Name: dataset_metadata; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE dataset_metadata (
    dataset_type_id bigint NOT NULL,
    dataset_id bigint NOT NULL,
    metadata_xml xml NOT NULL
);


ALTER TABLE public.dataset_metadata OWNER TO cube_admin;

--
-- TOC entry 4545 (class 0 OID 0)
-- Dependencies: 180
-- Name: TABLE dataset_metadata; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE dataset_metadata IS 'Data: Lookup table for dataset-level metadata (one:one)';


--
-- TOC entry 4546 (class 0 OID 0)
-- Dependencies: 180
-- Name: COLUMN dataset_metadata.dataset_type_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN dataset_metadata.dataset_type_id IS 'Part of composite foreign key to dataset (other key is dataset_id). Also indirect reference to dataset_type';


--
-- TOC entry 4547 (class 0 OID 0)
-- Dependencies: 180
-- Name: COLUMN dataset_metadata.dataset_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN dataset_metadata.dataset_id IS 'Part of composite foreign key to dataset (other key is dataset_type_id).';


--
-- TOC entry 4548 (class 0 OID 0)
-- Dependencies: 180
-- Name: COLUMN dataset_metadata.metadata_xml; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN dataset_metadata.metadata_xml IS 'XML metadata harvested from source dataset';


--
-- TOC entry 181 (class 1259 OID 3524037)
-- Name: dataset_type; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE dataset_type (
    dataset_type_id bigint NOT NULL,
    dataset_type_name character varying(254),
    dataset_type_tag character varying(32)
);


ALTER TABLE public.dataset_type OWNER TO cube_admin;

--
-- TOC entry 4550 (class 0 OID 0)
-- Dependencies: 181
-- Name: TABLE dataset_type; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE dataset_type IS 'Configuration: Type of source dataset (processing level)';


--
-- TOC entry 4551 (class 0 OID 0)
-- Dependencies: 181
-- Name: COLUMN dataset_type.dataset_type_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN dataset_type.dataset_type_id IS 'Primary key for dataset_type.';


--
-- TOC entry 4552 (class 0 OID 0)
-- Dependencies: 181
-- Name: COLUMN dataset_type.dataset_type_name; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN dataset_type.dataset_type_name IS 'Long name for dataset type.';


--
-- TOC entry 4553 (class 0 OID 0)
-- Dependencies: 181
-- Name: COLUMN dataset_type.dataset_type_tag; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN dataset_type.dataset_type_tag IS 'Short tag (candidate key) for dataset_type';


--
-- TOC entry 182 (class 1259 OID 3524040)
-- Name: dataset_type_dimension; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE dataset_type_dimension (
    dataset_type_id bigint NOT NULL,
    domain_id bigint NOT NULL,
    dimension_id bigint NOT NULL,
    dimension_order smallint NOT NULL,
    reverse_index boolean DEFAULT false NOT NULL
);


ALTER TABLE public.dataset_type_dimension OWNER TO cube_admin;

--
-- TOC entry 4555 (class 0 OID 0)
-- Dependencies: 182
-- Name: TABLE dataset_type_dimension; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE dataset_type_dimension IS 'Configuration: Association between dataset type and dimensions. Used to define dimensionality of source dataset types';


--
-- TOC entry 4556 (class 0 OID 0)
-- Dependencies: 182
-- Name: COLUMN dataset_type_dimension.dataset_type_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN dataset_type_dimension.dataset_type_id IS 'Part of composite primary key (other keys are domain_id and dimension_id). Foreign key to dataset_type.';


--
-- TOC entry 4557 (class 0 OID 0)
-- Dependencies: 182
-- Name: COLUMN dataset_type_dimension.domain_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN dataset_type_dimension.domain_id IS 'Part of composite primary key (other keys are dataset_type_id and dimension_id). Foreign key to domain';


--
-- TOC entry 4558 (class 0 OID 0)
-- Dependencies: 182
-- Name: COLUMN dataset_type_dimension.dimension_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN dataset_type_dimension.dimension_id IS 'Part of composite primary key (other keys are dataset_type_id and domain_id). Foreign key to dimension.';


--
-- TOC entry 4559 (class 0 OID 0)
-- Dependencies: 182
-- Name: COLUMN dataset_type_dimension.dimension_order; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN dataset_type_dimension.dimension_order IS 'Order in which dimensions are arranged in each dataset_type';


--
-- TOC entry 4560 (class 0 OID 0)
-- Dependencies: 182
-- Name: COLUMN dataset_type_dimension.reverse_index; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN dataset_type_dimension.reverse_index IS 'Boolean flag indicating whether sense of indexing is reversed (e.g. Y axis for imagery)';


--
-- TOC entry 183 (class 1259 OID 3524043)
-- Name: dataset_type_domain; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE dataset_type_domain (
    dataset_type_id bigint NOT NULL,
    domain_id bigint NOT NULL,
    reference_system_id bigint
);


ALTER TABLE public.dataset_type_domain OWNER TO cube_admin;

--
-- TOC entry 4562 (class 0 OID 0)
-- Dependencies: 183
-- Name: TABLE dataset_type_domain; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE dataset_type_domain IS 'Configuration: Association between dataset types and domains (many-many).
Used to define which domains cover a given dataset type';


--
-- TOC entry 4563 (class 0 OID 0)
-- Dependencies: 183
-- Name: COLUMN dataset_type_domain.dataset_type_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN dataset_type_domain.dataset_type_id IS 'Part of composite primary key (other key is domain_id). Foreign key to dataset_type.';


--
-- TOC entry 4564 (class 0 OID 0)
-- Dependencies: 183
-- Name: COLUMN dataset_type_domain.domain_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN dataset_type_domain.domain_id IS 'Part of composite primary key (other key is dataset_type_id). Foreign key to domain.';


--
-- TOC entry 4565 (class 0 OID 0)
-- Dependencies: 183
-- Name: COLUMN dataset_type_domain.reference_system_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN dataset_type_domain.reference_system_id IS 'Foreign key to reference_system';


--
-- TOC entry 184 (class 1259 OID 3524046)
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
-- TOC entry 4567 (class 0 OID 0)
-- Dependencies: 184
-- Name: TABLE dataset_type_measurement_type; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE dataset_type_measurement_type IS 'Configuration: Associations between dataset types and measurement types (one-many)
e.g. associations between Landsat 7 NBAR and specific surface-reflectance corrected Landsat 7 bands';


--
-- TOC entry 4568 (class 0 OID 0)
-- Dependencies: 184
-- Name: COLUMN dataset_type_measurement_type.dataset_type_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN dataset_type_measurement_type.dataset_type_id IS 'Part of composite primary key (other keys are measurement_metatype_id and measurement_type_id). Foreign key to dataset_type.';


--
-- TOC entry 4569 (class 0 OID 0)
-- Dependencies: 184
-- Name: COLUMN dataset_type_measurement_type.measurement_metatype_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN dataset_type_measurement_type.measurement_metatype_id IS 'Part of composite primary key (other keys are dataset_type_id and measurement_type_id).  Part of composite foreign key to measurement_type. Indirect reference to measurement_metatype.';


--
-- TOC entry 4570 (class 0 OID 0)
-- Dependencies: 184
-- Name: COLUMN dataset_type_measurement_type.measurement_type_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN dataset_type_measurement_type.measurement_type_id IS 'Part of composite primary key (other keys are dataset_type_id and measurement_type_id). Part of composite foreign key to measurement_type.';


--
-- TOC entry 4571 (class 0 OID 0)
-- Dependencies: 184
-- Name: COLUMN dataset_type_measurement_type.datatype_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN dataset_type_measurement_type.datatype_id IS 'Foreign key to datatype.';


--
-- TOC entry 4572 (class 0 OID 0)
-- Dependencies: 184
-- Name: COLUMN dataset_type_measurement_type.measurement_type_index; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN dataset_type_measurement_type.measurement_type_index IS 'Order in which measurement type is stored in source dataset file';


--
-- TOC entry 185 (class 1259 OID 3524049)
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
-- TOC entry 4574 (class 0 OID 0)
-- Dependencies: 185
-- Name: TABLE datatype; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE datatype IS 'Configuration: Lookup table for measurement_type datatypes.';


--
-- TOC entry 4575 (class 0 OID 0)
-- Dependencies: 185
-- Name: COLUMN datatype.datatype_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN datatype.datatype_id IS 'Primary key for datatype';


--
-- TOC entry 4576 (class 0 OID 0)
-- Dependencies: 185
-- Name: COLUMN datatype.datatype_name; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN datatype.datatype_name IS 'Long name of datatype';


--
-- TOC entry 4577 (class 0 OID 0)
-- Dependencies: 185
-- Name: COLUMN datatype.numpy_datatype_name; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN datatype.numpy_datatype_name IS 'Text representation of numpy datatype';


--
-- TOC entry 4578 (class 0 OID 0)
-- Dependencies: 185
-- Name: COLUMN datatype.gdal_datatype_name; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN datatype.gdal_datatype_name IS 'Text representation of GDAL datatype';


--
-- TOC entry 4579 (class 0 OID 0)
-- Dependencies: 185
-- Name: COLUMN datatype.netcdf_datatype_name; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN datatype.netcdf_datatype_name IS 'Text representation of netCDF datatype';


--
-- TOC entry 186 (class 1259 OID 3524052)
-- Name: dimension; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE dimension (
    dimension_id bigint NOT NULL,
    dimension_name character varying(50) NOT NULL,
    dimension_tag character varying(8) NOT NULL
);


ALTER TABLE public.dimension OWNER TO cube_admin;

--
-- TOC entry 4581 (class 0 OID 0)
-- Dependencies: 186
-- Name: TABLE dimension; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE dimension IS 'Configuration: Dimensions for n-dimensional data structures, e.g. x,y,z,t';


--
-- TOC entry 4582 (class 0 OID 0)
-- Dependencies: 186
-- Name: COLUMN dimension.dimension_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN dimension.dimension_id IS 'Primary key for dimension';


--
-- TOC entry 4583 (class 0 OID 0)
-- Dependencies: 186
-- Name: COLUMN dimension.dimension_name; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN dimension.dimension_name IS 'Long name for dimension';


--
-- TOC entry 4584 (class 0 OID 0)
-- Dependencies: 186
-- Name: COLUMN dimension.dimension_tag; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN dimension.dimension_tag IS 'Short tag (candidate key) for dimension.';


--
-- TOC entry 187 (class 1259 OID 3524055)
-- Name: dimension_domain; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE dimension_domain (
    domain_id bigint NOT NULL,
    dimension_id bigint NOT NULL
);


ALTER TABLE public.dimension_domain OWNER TO cube_admin;

--
-- TOC entry 4586 (class 0 OID 0)
-- Dependencies: 187
-- Name: TABLE dimension_domain; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE dimension_domain IS 'Configuration: Many-many  mapping between dimensions and domains to allow multiple dimensions to be included in multiple domains.
For example, the z dimension could be managed in a Z-spatial domain, or in an XYZ-spatial domain.';


--
-- TOC entry 4587 (class 0 OID 0)
-- Dependencies: 187
-- Name: COLUMN dimension_domain.domain_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN dimension_domain.domain_id IS 'Foreign key to domain';


--
-- TOC entry 4588 (class 0 OID 0)
-- Dependencies: 187
-- Name: COLUMN dimension_domain.dimension_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN dimension_domain.dimension_id IS 'Foreign key to dimension.';


--
-- TOC entry 188 (class 1259 OID 3524058)
-- Name: domain; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE domain (
    domain_id bigint NOT NULL,
    domain_name character varying(16),
    domain_tag character varying(16)
);


ALTER TABLE public.domain OWNER TO cube_admin;

--
-- TOC entry 4590 (class 0 OID 0)
-- Dependencies: 188
-- Name: TABLE domain; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE domain IS 'Configuration: Domain groupings of dimensions (e.g. spectral, spatial XY, spatial XYZ, temporal)';


--
-- TOC entry 4591 (class 0 OID 0)
-- Dependencies: 188
-- Name: COLUMN domain.domain_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN domain.domain_id IS 'Primary key for domain';


--
-- TOC entry 4592 (class 0 OID 0)
-- Dependencies: 188
-- Name: COLUMN domain.domain_name; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN domain.domain_name IS 'Long name for domain';


--
-- TOC entry 4593 (class 0 OID 0)
-- Dependencies: 188
-- Name: COLUMN domain.domain_tag; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN domain.domain_tag IS 'Short tag (candidate key) for domain.';


--
-- TOC entry 189 (class 1259 OID 3524061)
-- Name: indexing_type; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE indexing_type (
    indexing_type_id smallint NOT NULL,
    indexing_type_name character varying(128)
);


ALTER TABLE public.indexing_type OWNER TO cube_admin;

--
-- TOC entry 4595 (class 0 OID 0)
-- Dependencies: 189
-- Name: TABLE indexing_type; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE indexing_type IS 'Configuration: Lookup table to manage what kind of indexing to apply to a given dimension';


--
-- TOC entry 4596 (class 0 OID 0)
-- Dependencies: 189
-- Name: COLUMN indexing_type.indexing_type_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN indexing_type.indexing_type_id IS 'Primary key for indexing_type';


--
-- TOC entry 4597 (class 0 OID 0)
-- Dependencies: 189
-- Name: COLUMN indexing_type.indexing_type_name; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN indexing_type.indexing_type_name IS 'Name of indexing type.
Types include regular (e.g. lat/lon), irregular (e.g. time) and fixed (e.g. bands)';


--
-- TOC entry 190 (class 1259 OID 3524064)
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
-- TOC entry 4599 (class 0 OID 0)
-- Dependencies: 190
-- Name: TABLE reference_system; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE reference_system IS 'Configuration: Coordinate reference systems for aplication to specific domains.
e.g. EPSG:4326, seconds since 1/1/1970 0:00, etc.';


--
-- TOC entry 4600 (class 0 OID 0)
-- Dependencies: 190
-- Name: COLUMN reference_system.reference_system_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN reference_system.reference_system_id IS 'Primary key for reference_system';


--
-- TOC entry 4601 (class 0 OID 0)
-- Dependencies: 190
-- Name: COLUMN reference_system.reference_system_name; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN reference_system.reference_system_name IS 'Long name for reference_system';


--
-- TOC entry 4602 (class 0 OID 0)
-- Dependencies: 190
-- Name: COLUMN reference_system.reference_system_unit; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN reference_system.reference_system_unit IS 'Unit for reference_system';


--
-- TOC entry 4603 (class 0 OID 0)
-- Dependencies: 190
-- Name: COLUMN reference_system.reference_system_definition; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN reference_system.reference_system_definition IS 'Textual definition of reference system.';


--
-- TOC entry 4604 (class 0 OID 0)
-- Dependencies: 190
-- Name: COLUMN reference_system.reference_system_tag; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN reference_system.reference_system_tag IS 'Short tag (candidate_key) for reference_system.';


--
-- TOC entry 191 (class 1259 OID 3524067)
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
-- TOC entry 4606 (class 0 OID 0)
-- Dependencies: 191
-- Name: TABLE reference_system_indexing; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE reference_system_indexing IS 'Configuration: Optional non-linear indexing for dimension in a given domain.
e.g. A spectral dimension containing multple bands needs to be indexed by band number, and each band number can be associated with a given measurement_type.';


--
-- TOC entry 4607 (class 0 OID 0)
-- Dependencies: 191
-- Name: COLUMN reference_system_indexing.reference_system_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN reference_system_indexing.reference_system_id IS 'Part of composite primary key for reference_system_indexing (other key is array_index). Foreign key to reference_system.';


--
-- TOC entry 4608 (class 0 OID 0)
-- Dependencies: 191
-- Name: COLUMN reference_system_indexing.array_index; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN reference_system_indexing.array_index IS 'Part of composite primary key for reference_system_indexing (other key is reference_system_id). Zero-based array index for array dimension (e.g. spectral band).';


--
-- TOC entry 4609 (class 0 OID 0)
-- Dependencies: 191
-- Name: COLUMN reference_system_indexing.indexing_name; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN reference_system_indexing.indexing_name IS 'NFI - what was I thinking?';


--
-- TOC entry 4610 (class 0 OID 0)
-- Dependencies: 191
-- Name: COLUMN reference_system_indexing.measurement_metatype_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN reference_system_indexing.measurement_metatype_id IS 'Part of foreign key to measurement_type (other key is measurement_type_id). Also indirect reference to measurement_metatype.';


--
-- TOC entry 4611 (class 0 OID 0)
-- Dependencies: 191
-- Name: COLUMN reference_system_indexing.measurement_type_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN reference_system_indexing.measurement_type_id IS 'Part of foreign key to measurement_type (other key is measurement_metatype_id).';


--
-- TOC entry 192 (class 1259 OID 3524070)
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
-- TOC entry 4613 (class 0 OID 0)
-- Dependencies: 192
-- Name: TABLE storage_type; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE storage_type IS 'Configuration: storage parameter lookup table. Used TO manage different storage_types';


--
-- TOC entry 4614 (class 0 OID 0)
-- Dependencies: 192
-- Name: COLUMN storage_type.storage_type_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN storage_type.storage_type_id IS 'Primary key for storage_type.';


--
-- TOC entry 4615 (class 0 OID 0)
-- Dependencies: 192
-- Name: COLUMN storage_type.storage_type_name; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN storage_type.storage_type_name IS 'Long name for storage_type.';


--
-- TOC entry 4616 (class 0 OID 0)
-- Dependencies: 192
-- Name: COLUMN storage_type.storage_type_tag; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN storage_type.storage_type_tag IS 'Short tag (candidate key) for storage_type.';


--
-- TOC entry 4617 (class 0 OID 0)
-- Dependencies: 192
-- Name: COLUMN storage_type.storage_type_location; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN storage_type.storage_type_location IS 'Root directory for each storage_type';


--
-- TOC entry 193 (class 1259 OID 3524076)
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
-- TOC entry 4619 (class 0 OID 0)
-- Dependencies: 193
-- Name: TABLE storage_type_dimension; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE storage_type_dimension IS 'Configuration: Association between storage type and dimensions. Used TO define dimensionality of storage type';


--
-- TOC entry 4620 (class 0 OID 0)
-- Dependencies: 193
-- Name: COLUMN storage_type_dimension.storage_type_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN storage_type_dimension.storage_type_id IS 'Part of composite primary key for storage_type_dimension (Other keys are domain_id, dimension_id). Foreign key to storage_type.';


--
-- TOC entry 4621 (class 0 OID 0)
-- Dependencies: 193
-- Name: COLUMN storage_type_dimension.domain_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN storage_type_dimension.domain_id IS 'Part of composite primary key for storage_type_dimension (Other keys are storage_type_id, dimension_id). Foreign key to dimension_domain and indirect reference to domain.';


--
-- TOC entry 4622 (class 0 OID 0)
-- Dependencies: 193
-- Name: COLUMN storage_type_dimension.dimension_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN storage_type_dimension.dimension_id IS 'Part of composite primary key for storage_type_dimension (Other keys are storage_type_id, domain_id). Foreign key to dimension_domain and indirect reference to dimension.';


--
-- TOC entry 4623 (class 0 OID 0)
-- Dependencies: 193
-- Name: COLUMN storage_type_dimension.dimension_order; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN storage_type_dimension.dimension_order IS 'Order of dimension in storage type. Should be 1-based sequence but increments are not important, only sort order.';


--
-- TOC entry 4624 (class 0 OID 0)
-- Dependencies: 193
-- Name: COLUMN storage_type_dimension.dimension_extent; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN storage_type_dimension.dimension_extent IS 'Size of storage units along each dimension expressed in reference system units.';


--
-- TOC entry 4625 (class 0 OID 0)
-- Dependencies: 193
-- Name: COLUMN storage_type_dimension.dimension_elements; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN storage_type_dimension.dimension_elements IS 'Number of elements along each dimension for regularly indexed dimensions. Ignored for irregularly indexed dimensions such as time.';


--
-- TOC entry 4626 (class 0 OID 0)
-- Dependencies: 193
-- Name: COLUMN storage_type_dimension.dimension_cache; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN storage_type_dimension.dimension_cache IS 'Caching (e.g. netCDF chunk) size in each dimension.';


--
-- TOC entry 4627 (class 0 OID 0)
-- Dependencies: 193
-- Name: COLUMN storage_type_dimension.dimension_origin; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN storage_type_dimension.dimension_origin IS 'Origin of storage unit indexing scheme expressed in reference system units.';


--
-- TOC entry 4628 (class 0 OID 0)
-- Dependencies: 193
-- Name: COLUMN storage_type_dimension.indexing_type_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN storage_type_dimension.indexing_type_id IS 'Foreign key to indexing_type lookup table.';


--
-- TOC entry 4629 (class 0 OID 0)
-- Dependencies: 193
-- Name: COLUMN storage_type_dimension.reference_system_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN storage_type_dimension.reference_system_id IS 'Foreign key to reference_system lookup table for intra-storage-unit indexing.';


--
-- TOC entry 4630 (class 0 OID 0)
-- Dependencies: 193
-- Name: COLUMN storage_type_dimension.index_reference_system_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN storage_type_dimension.index_reference_system_id IS 'Foreign key to reference_system lookup table for external storage unit indexing.';


--
-- TOC entry 4631 (class 0 OID 0)
-- Dependencies: 193
-- Name: COLUMN storage_type_dimension.reverse_index; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN storage_type_dimension.reverse_index IS 'Flag indicating whether sense of indexing values should be the reverse of the array indices (e.g. Latitude with spatial origin in UL corner)';


--
-- TOC entry 194 (class 1259 OID 3524080)
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
-- TOC entry 195 (class 1259 OID 3524088)
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
-- TOC entry 196 (class 1259 OID 3524092)
-- Name: property; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE property (
    property_id bigint NOT NULL,
    property_name character varying(32),
    datatype_id smallint
);


ALTER TABLE public.property OWNER TO cube_admin;

--
-- TOC entry 4635 (class 0 OID 0)
-- Dependencies: 196
-- Name: TABLE property; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE property IS 'Configuration: Lookup table for properties which can have an associated value in metadata.';


--
-- TOC entry 4636 (class 0 OID 0)
-- Dependencies: 196
-- Name: COLUMN property.property_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN property.property_id IS 'Primary key for property';


--
-- TOC entry 4637 (class 0 OID 0)
-- Dependencies: 196
-- Name: COLUMN property.property_name; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN property.property_name IS 'Name of property';


--
-- TOC entry 4638 (class 0 OID 0)
-- Dependencies: 196
-- Name: COLUMN property.datatype_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN property.datatype_id IS 'Foreign key to datatype';


--
-- TOC entry 197 (class 1259 OID 3524095)
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
-- TOC entry 4640 (class 0 OID 0)
-- Dependencies: 197
-- Name: TABLE storage_type_dimension_property; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE storage_type_dimension_property IS 'Configuration: Metadata properties of dimension in storage type';


--
-- TOC entry 4641 (class 0 OID 0)
-- Dependencies: 197
-- Name: COLUMN storage_type_dimension_property.storage_type_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN storage_type_dimension_property.storage_type_id IS 'Part of composite foreign key to storage_type_dimension (other keys are domain_id and dimension_id). Also indirect reference to storage_type.';


--
-- TOC entry 4642 (class 0 OID 0)
-- Dependencies: 197
-- Name: COLUMN storage_type_dimension_property.domain_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN storage_type_dimension_property.domain_id IS 'Part of composite foreign key to storage_type_dimension (other keys are storage_type_id and dimension_id). Also indirect reference to dimension_domain and domain.';


--
-- TOC entry 4643 (class 0 OID 0)
-- Dependencies: 197
-- Name: COLUMN storage_type_dimension_property.dimension_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN storage_type_dimension_property.dimension_id IS 'Part of composite foreign key to storage_type_dimension (other keys are storage_type_id and domain_id). Also indirect reference to dimension_domain and dimension.';


--
-- TOC entry 4644 (class 0 OID 0)
-- Dependencies: 197
-- Name: COLUMN storage_type_dimension_property.property_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN storage_type_dimension_property.property_id IS 'Foreign key to property lookup table';


--
-- TOC entry 4645 (class 0 OID 0)
-- Dependencies: 197
-- Name: COLUMN storage_type_dimension_property.attribute_string; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN storage_type_dimension_property.attribute_string IS 'String representation of attribute value';


--
-- TOC entry 198 (class 1259 OID 3524098)
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
-- TOC entry 199 (class 1259 OID 3524103)
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
-- TOC entry 4648 (class 0 OID 0)
-- Dependencies: 199
-- Name: TABLE instrument; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE instrument IS 'Configuration: Instrument used to gather observations.
An example would be the ETM+ sensor on the Landsat 7 platform';


--
-- TOC entry 4649 (class 0 OID 0)
-- Dependencies: 199
-- Name: COLUMN instrument.instrument_type_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN instrument.instrument_type_id IS 'Part of compound primary key for instrument (other key is instrument_id). Foreign key to instrument_type';


--
-- TOC entry 4650 (class 0 OID 0)
-- Dependencies: 199
-- Name: COLUMN instrument.instrument_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN instrument.instrument_id IS 'Part of compound primary key for instrument (other key is instrument_type_id).';


--
-- TOC entry 4651 (class 0 OID 0)
-- Dependencies: 199
-- Name: COLUMN instrument.instrument_name; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN instrument.instrument_name IS 'Name of instrument';


--
-- TOC entry 4652 (class 0 OID 0)
-- Dependencies: 199
-- Name: COLUMN instrument.platform_type_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN instrument.platform_type_id IS 'Partial foreign key to platform (other key is platform_id). Also indirect reference to platform_type.';


--
-- TOC entry 4653 (class 0 OID 0)
-- Dependencies: 199
-- Name: COLUMN instrument.platform_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN instrument.platform_id IS 'Partial foreign key to platform (other key is platform_type_id).';


--
-- TOC entry 4654 (class 0 OID 0)
-- Dependencies: 199
-- Name: COLUMN instrument.instrument_tag; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN instrument.instrument_tag IS 'Short tag (candidate key) for instrument.';


--
-- TOC entry 200 (class 1259 OID 3524106)
-- Name: instrument_type; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE instrument_type (
    instrument_type_id bigint NOT NULL,
    instrument_type_name character varying(128)
);


ALTER TABLE public.instrument_type OWNER TO cube_admin;

--
-- TOC entry 4656 (class 0 OID 0)
-- Dependencies: 200
-- Name: TABLE instrument_type; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE instrument_type IS 'Configuration: Lookup table for instrument category';


--
-- TOC entry 4657 (class 0 OID 0)
-- Dependencies: 200
-- Name: COLUMN instrument_type.instrument_type_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN instrument_type.instrument_type_id IS 'Primary key for instrument_type.';


--
-- TOC entry 4658 (class 0 OID 0)
-- Dependencies: 200
-- Name: COLUMN instrument_type.instrument_type_name; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN instrument_type.instrument_type_name IS 'Name of instrument_type.';


--
-- TOC entry 201 (class 1259 OID 3524109)
-- Name: measurement_metatype; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE measurement_metatype (
    measurement_metatype_id bigint NOT NULL,
    measurement_metatype_name character varying(254)
);


ALTER TABLE public.measurement_metatype OWNER TO cube_admin;

--
-- TOC entry 4660 (class 0 OID 0)
-- Dependencies: 201
-- Name: TABLE measurement_metatype; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE measurement_metatype IS 'Configuration: Lookup table for general type of measurement (e.g. spectral band)';


--
-- TOC entry 4661 (class 0 OID 0)
-- Dependencies: 201
-- Name: COLUMN measurement_metatype.measurement_metatype_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN measurement_metatype.measurement_metatype_id IS 'Primary key for measurement_metatype';


--
-- TOC entry 4662 (class 0 OID 0)
-- Dependencies: 201
-- Name: COLUMN measurement_metatype.measurement_metatype_name; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN measurement_metatype.measurement_metatype_name IS 'Name of measurement metatype.';


--
-- TOC entry 202 (class 1259 OID 3524112)
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
-- TOC entry 4664 (class 0 OID 0)
-- Dependencies: 202
-- Name: TABLE measurement_type; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE measurement_type IS 'Configuration: Description of measurement(s) held in n-dimensional data structures: e.g. bands';


--
-- TOC entry 4665 (class 0 OID 0)
-- Dependencies: 202
-- Name: COLUMN measurement_type.measurement_metatype_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN measurement_type.measurement_metatype_id IS 'Part of compound primary key for measurement_type (other key is measurement_type_id). Foreign key to measurement_metatype.';


--
-- TOC entry 4666 (class 0 OID 0)
-- Dependencies: 202
-- Name: COLUMN measurement_type.measurement_type_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN measurement_type.measurement_type_id IS 'Part of compound primary key for measurement_type (other key is measurement_metatype_id).';


--
-- TOC entry 4667 (class 0 OID 0)
-- Dependencies: 202
-- Name: COLUMN measurement_type.measurement_type_name; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN measurement_type.measurement_type_name IS 'Long name of measurement type';


--
-- TOC entry 4668 (class 0 OID 0)
-- Dependencies: 202
-- Name: COLUMN measurement_type.measurement_type_tag; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN measurement_type.measurement_type_tag IS 'Short tag (candidate_key) for measurement_type';


--
-- TOC entry 203 (class 1259 OID 3524115)
-- Name: observation; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE observation (
    observation_type_id bigint NOT NULL,
    observation_id bigint NOT NULL,
    observation_start_datetime timestamp with time zone,
    observation_end_datetime timestamp with time zone,
    instrument_type_id bigint,
    instrument_id bigint,
    observation_reference character varying(128)
);


ALTER TABLE public.observation OWNER TO cube_admin;

--
-- TOC entry 4670 (class 0 OID 0)
-- Dependencies: 203
-- Name: TABLE observation; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE observation IS 'Data: Source observation for datasets.
Analagous to old "acquisition" table in AGDC version 0 DB';


--
-- TOC entry 4671 (class 0 OID 0)
-- Dependencies: 203
-- Name: COLUMN observation.observation_type_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN observation.observation_type_id IS 'Part of compound primary key for observation (other key is observation_id). Foreign key to observation_type.';


--
-- TOC entry 4672 (class 0 OID 0)
-- Dependencies: 203
-- Name: COLUMN observation.observation_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN observation.observation_id IS 'Part of compound primary key for observation (other key is observation_type_id).';


--
-- TOC entry 4673 (class 0 OID 0)
-- Dependencies: 203
-- Name: COLUMN observation.observation_start_datetime; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN observation.observation_start_datetime IS 'Start datetime for observation.';


--
-- TOC entry 4674 (class 0 OID 0)
-- Dependencies: 203
-- Name: COLUMN observation.observation_end_datetime; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN observation.observation_end_datetime IS 'End datetime for observation.';


--
-- TOC entry 4675 (class 0 OID 0)
-- Dependencies: 203
-- Name: COLUMN observation.instrument_type_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN observation.instrument_type_id IS 'Part of foreign key to instrument (other key is instrument_id). Also indirect reference to instrument_type.';


--
-- TOC entry 4676 (class 0 OID 0)
-- Dependencies: 203
-- Name: COLUMN observation.instrument_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN observation.instrument_id IS 'Part of foreign key to instrument (other key is instrument_type_id).';


--
-- TOC entry 4677 (class 0 OID 0)
-- Dependencies: 203
-- Name: COLUMN observation.observation_reference; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN observation.observation_reference IS 'Unique reference for observation (e.g. Landsat Path-Row-Date)';


--
-- TOC entry 204 (class 1259 OID 3524118)
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
-- TOC entry 205 (class 1259 OID 3524120)
-- Name: observation_type; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE observation_type (
    observation_type_id bigint NOT NULL,
    observation_type_name character varying(254)
);


ALTER TABLE public.observation_type OWNER TO cube_admin;

--
-- TOC entry 4679 (class 0 OID 0)
-- Dependencies: 205
-- Name: TABLE observation_type; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE observation_type IS 'Configuration: Lookup table for type of source observation';


--
-- TOC entry 4680 (class 0 OID 0)
-- Dependencies: 205
-- Name: COLUMN observation_type.observation_type_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN observation_type.observation_type_id IS 'Primary key for observation type';


--
-- TOC entry 4681 (class 0 OID 0)
-- Dependencies: 205
-- Name: COLUMN observation_type.observation_type_name; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN observation_type.observation_type_name IS 'Long name for observation type';


--
-- TOC entry 206 (class 1259 OID 3524123)
-- Name: platform; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE platform (
    platform_type_id bigint NOT NULL,
    platform_id bigint NOT NULL,
    platform_name character varying(128),
    platform_tag character varying(16)
);


ALTER TABLE public.platform OWNER TO cube_admin;

--
-- TOC entry 4683 (class 0 OID 0)
-- Dependencies: 206
-- Name: TABLE platform; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE platform IS 'Configuration: Platform on which instrument is mounted.
An example would be a specific satellite such as Landsat 7';


--
-- TOC entry 4684 (class 0 OID 0)
-- Dependencies: 206
-- Name: COLUMN platform.platform_type_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN platform.platform_type_id IS 'Part of composite primary key for platform (other key is platform_id). Also indirect reference to platform_type.';


--
-- TOC entry 4685 (class 0 OID 0)
-- Dependencies: 206
-- Name: COLUMN platform.platform_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN platform.platform_id IS 'Part of composite primary key for platform (other key is platform_type_id). ';


--
-- TOC entry 4686 (class 0 OID 0)
-- Dependencies: 206
-- Name: COLUMN platform.platform_name; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN platform.platform_name IS 'Long name for platform';


--
-- TOC entry 4687 (class 0 OID 0)
-- Dependencies: 206
-- Name: COLUMN platform.platform_tag; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN platform.platform_tag IS 'Short tag (candidate key) for platform';


--
-- TOC entry 207 (class 1259 OID 3524126)
-- Name: platform_type; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE platform_type (
    platform_type_id bigint NOT NULL,
    platform_type_name character varying(128)
);


ALTER TABLE public.platform_type OWNER TO cube_admin;

--
-- TOC entry 4689 (class 0 OID 0)
-- Dependencies: 207
-- Name: TABLE platform_type; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE platform_type IS 'Configuration: Lookup table for platform category
e.g. Satellite or Ship';


--
-- TOC entry 4690 (class 0 OID 0)
-- Dependencies: 207
-- Name: COLUMN platform_type.platform_type_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN platform_type.platform_type_id IS 'Primary key for platform_type';


--
-- TOC entry 4691 (class 0 OID 0)
-- Dependencies: 207
-- Name: COLUMN platform_type.platform_type_name; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN platform_type.platform_type_name IS 'Name of platform_type';


--
-- TOC entry 232 (class 1259 OID 3525949)
-- Name: spatial_footprint; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE TABLE spatial_footprint (
    spatial_footprint_id bigint NOT NULL,
    spatial_footprint_geometry geometry NOT NULL
);


ALTER TABLE public.spatial_footprint OWNER TO cube_admin;

--
-- TOC entry 4693 (class 0 OID 0)
-- Dependencies: 232
-- Name: TABLE spatial_footprint; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE spatial_footprint IS 'Data: Spatial footprint associated with storage units.';


--
-- TOC entry 4694 (class 0 OID 0)
-- Dependencies: 232
-- Name: COLUMN spatial_footprint.spatial_footprint_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN spatial_footprint.spatial_footprint_id IS 'Primary key for spatial footprint';


--
-- TOC entry 4695 (class 0 OID 0)
-- Dependencies: 232
-- Name: COLUMN spatial_footprint.spatial_footprint_geometry; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN spatial_footprint.spatial_footprint_geometry IS 'PostGIS geometry for storage unit footprint';


--
-- TOC entry 208 (class 1259 OID 3524129)
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
-- TOC entry 4696 (class 0 OID 0)
-- Dependencies: 208
-- Name: TABLE storage; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE storage IS 'Data: n-dimensional data structure instances';


--
-- TOC entry 4697 (class 0 OID 0)
-- Dependencies: 208
-- Name: COLUMN storage.storage_type_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN storage.storage_type_id IS 'Part of compound primary key for storage (other keys are storage_id and storage_version). Also indirect reference to storage_type.';


--
-- TOC entry 4698 (class 0 OID 0)
-- Dependencies: 208
-- Name: COLUMN storage.storage_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN storage.storage_id IS 'Part of compound primary key for storage (other keys are storage_type_id and storage_version).';


--
-- TOC entry 4699 (class 0 OID 0)
-- Dependencies: 208
-- Name: COLUMN storage.storage_version; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN storage.storage_version IS 'Part of compound primary key for storage (other keys are storage_type_id and storage_id). Should be zero for current version to keep queries simple.';


--
-- TOC entry 4700 (class 0 OID 0)
-- Dependencies: 208
-- Name: COLUMN storage.storage_location; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN storage.storage_location IS 'Partial path to storage unit file. Must be appended to storage_type.storage_type_location to create fully qualified path.';


--
-- TOC entry 4701 (class 0 OID 0)
-- Dependencies: 208
-- Name: COLUMN storage.md5_checksum; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN storage.md5_checksum IS 'MD5 checksum for storage unit file';


--
-- TOC entry 4702 (class 0 OID 0)
-- Dependencies: 208
-- Name: COLUMN storage.storage_bytes; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN storage.storage_bytes IS 'Number of bytes in storage unit file';


--
-- TOC entry 4703 (class 0 OID 0)
-- Dependencies: 208
-- Name: COLUMN storage.spatial_footprint_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN storage.spatial_footprint_id IS 'Foreign key to spatial_footprint';


--
-- TOC entry 209 (class 1259 OID 3524132)
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
-- TOC entry 4705 (class 0 OID 0)
-- Dependencies: 209
-- Name: TABLE storage_dataset; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE storage_dataset IS 'Data: Association between storage and dataset instances (many-many)';


--
-- TOC entry 4706 (class 0 OID 0)
-- Dependencies: 209
-- Name: COLUMN storage_dataset.storage_type_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN storage_dataset.storage_type_id IS 'Part of composite primary key for storage_dataset relational entity (Other keys are storage_id, storage_version, dataset_type_id, dataset_id). Part of composite foreign key to storage and indirect association with storage_type.';


--
-- TOC entry 4707 (class 0 OID 0)
-- Dependencies: 209
-- Name: COLUMN storage_dataset.storage_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN storage_dataset.storage_id IS 'Part of composite primary key for storage_dataset relational entity (Other keys are storage_type_id, storage_version, dataset_type_id, dataset_id). Part of composite foreign key to storage.';


--
-- TOC entry 4708 (class 0 OID 0)
-- Dependencies: 209
-- Name: COLUMN storage_dataset.storage_version; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN storage_dataset.storage_version IS 'Part of composite primary key for storage_dataset relational entity (Other keys are storage_type_id, storage_id, dataset_type_id, dataset_id). Part of composite foreign key to storage.';


--
-- TOC entry 4709 (class 0 OID 0)
-- Dependencies: 209
-- Name: COLUMN storage_dataset.dataset_type_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN storage_dataset.dataset_type_id IS 'Part of composite primary key for storage_dataset relational entity (Other keys are storage_type_id, storage_id, storage_version, dataset_id). Part of composite foreign key to dataset and indirect association with dataset_type.';


--
-- TOC entry 4710 (class 0 OID 0)
-- Dependencies: 209
-- Name: COLUMN storage_dataset.dataset_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN storage_dataset.dataset_id IS 'Part of composite primary key for storage_dataset relational entity (Other keys are storage_type_id, storage_id, storage_version, dataset_type_id). Part of composite foreign key to dataset.';


--
-- TOC entry 210 (class 1259 OID 3524135)
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
-- TOC entry 4712 (class 0 OID 0)
-- Dependencies: 210
-- Name: TABLE storage_dimension; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE storage_dimension IS 'Data: Association between storage and dimensions. Used to define attributes for each dimension in storage instances';


--
-- TOC entry 4713 (class 0 OID 0)
-- Dependencies: 210
-- Name: COLUMN storage_dimension.storage_type_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN storage_dimension.storage_type_id IS 'Part of composite primary key for storage_dimension relational entity (Other keys are storage_id, storage_version, domain_id, dimension_id). Part of composite foreign key to storage and indirect association with storage_type.';


--
-- TOC entry 4714 (class 0 OID 0)
-- Dependencies: 210
-- Name: COLUMN storage_dimension.storage_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN storage_dimension.storage_id IS 'Part of composite primary key for storage_dimension relational entity (Other keys are storage_type_id, storage_version, domain_id, dimension_id). Part of composite foreign key to storage.';


--
-- TOC entry 4715 (class 0 OID 0)
-- Dependencies: 210
-- Name: COLUMN storage_dimension.storage_version; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN storage_dimension.storage_version IS 'Part of composite primary key for storage_dimension relational entity (Other keys are storage_type_id, storage_id, domain_id, dimension_id). Part of composite foreign key to storage.';


--
-- TOC entry 4716 (class 0 OID 0)
-- Dependencies: 210
-- Name: COLUMN storage_dimension.domain_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN storage_dimension.domain_id IS 'Part of composite primary key for storage_dimension relational entity (Other keys are storage_type_id, storage_id, storage_version, dimension_id). Part of composite foreign key to storage_type_dimension and indirect reference to domain.';


--
-- TOC entry 4717 (class 0 OID 0)
-- Dependencies: 210
-- Name: COLUMN storage_dimension.dimension_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN storage_dimension.dimension_id IS 'Part of composite primary key for storage_dimension relational entity (Other keys are storage_type_id, storage_id, storage_version, domain_id). Part of composite foreign key to storage_type_dimension and indirect reference to dimension.';


--
-- TOC entry 4718 (class 0 OID 0)
-- Dependencies: 210
-- Name: COLUMN storage_dimension.storage_dimension_index; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN storage_dimension.storage_dimension_index IS 'Value used to index a portion of a dataset within a storage unit, e.g. timeslice reference time. May be null when a storage unit has more than one index value for a given dataset (e.g. lat/lon).';


--
-- TOC entry 4719 (class 0 OID 0)
-- Dependencies: 210
-- Name: COLUMN storage_dimension.storage_dimension_min; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN storage_dimension.storage_dimension_min IS 'Minimum indexing value for a portion of a dataset in a storage unit in a particular dimension.';


--
-- TOC entry 4720 (class 0 OID 0)
-- Dependencies: 210
-- Name: COLUMN storage_dimension.storage_dimension_max; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN storage_dimension.storage_dimension_max IS 'Maximum indexing value for a portion of a dataset in a storage unit in a particular dimension.';


--
-- TOC entry 211 (class 1259 OID 3524138)
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
-- TOC entry 212 (class 1259 OID 3524140)
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
-- TOC entry 4722 (class 0 OID 0)
-- Dependencies: 212
-- Name: TABLE storage_type_measurement_type; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON TABLE storage_type_measurement_type IS 'Configuration: Associations between n-dimensional data structure types and measurement types (i.e. variables) (many-many)';


--
-- TOC entry 4723 (class 0 OID 0)
-- Dependencies: 212
-- Name: COLUMN storage_type_measurement_type.storage_type_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN storage_type_measurement_type.storage_type_id IS 'Part of composite primary key (other keys are measurement_metatype_id and measurement_type_id). Also foreign key to storage type.';


--
-- TOC entry 4724 (class 0 OID 0)
-- Dependencies: 212
-- Name: COLUMN storage_type_measurement_type.measurement_metatype_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN storage_type_measurement_type.measurement_metatype_id IS 'Part of composite primary key (other keys are storage_type_id and measurement_type_id). Also foreign key to measurement_type and indirect reference to measurement_metatype.';


--
-- TOC entry 4725 (class 0 OID 0)
-- Dependencies: 212
-- Name: COLUMN storage_type_measurement_type.measurement_type_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN storage_type_measurement_type.measurement_type_id IS 'Part of composite primary key (other keys are storage_type_id and measurement_metatype_id). Also foreign key to measurement_type.';


--
-- TOC entry 4726 (class 0 OID 0)
-- Dependencies: 212
-- Name: COLUMN storage_type_measurement_type.datatype_id; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN storage_type_measurement_type.datatype_id IS 'Foreign key to datatype lookup table.';


--
-- TOC entry 4727 (class 0 OID 0)
-- Dependencies: 212
-- Name: COLUMN storage_type_measurement_type.measurement_type_index; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN storage_type_measurement_type.measurement_type_index IS 'Order of measurement_type in storage unit. 
N.B: May be superfluous.';


--
-- TOC entry 4728 (class 0 OID 0)
-- Dependencies: 212
-- Name: COLUMN storage_type_measurement_type.nodata_value; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON COLUMN storage_type_measurement_type.nodata_value IS 'Value used to indicate no-data in storage unit measurement type.';


SET search_path = earth_observation, pg_catalog;

--
-- TOC entry 4204 (class 2606 OID 3524144)
-- Name: pk_spectral_parameters; Type: CONSTRAINT; Schema: earth_observation; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY spectral_parameters
    ADD CONSTRAINT pk_spectral_parameters PRIMARY KEY (measurement_metatype_id, measurement_type_id);


SET search_path = public, pg_catalog;

--
-- TOC entry 4208 (class 2606 OID 3524146)
-- Name: pk_dataset; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dataset
    ADD CONSTRAINT pk_dataset PRIMARY KEY (dataset_type_id, dataset_id);


--
-- TOC entry 4212 (class 2606 OID 3524148)
-- Name: pk_dataset_dimension; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dataset_dimension
    ADD CONSTRAINT pk_dataset_dimension PRIMARY KEY (dataset_type_id, dataset_id, domain_id, dimension_id);


--
-- TOC entry 4215 (class 2606 OID 3524150)
-- Name: pk_dataset_metadata; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dataset_metadata
    ADD CONSTRAINT pk_dataset_metadata PRIMARY KEY (dataset_type_id, dataset_id);


--
-- TOC entry 4217 (class 2606 OID 3524152)
-- Name: pk_dataset_type; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dataset_type
    ADD CONSTRAINT pk_dataset_type PRIMARY KEY (dataset_type_id);


--
-- TOC entry 4225 (class 2606 OID 3524154)
-- Name: pk_dataset_type_dimension; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dataset_type_dimension
    ADD CONSTRAINT pk_dataset_type_dimension PRIMARY KEY (dataset_type_id, domain_id, dimension_id);


--
-- TOC entry 4232 (class 2606 OID 3524156)
-- Name: pk_dataset_type_domain; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dataset_type_domain
    ADD CONSTRAINT pk_dataset_type_domain PRIMARY KEY (dataset_type_id, domain_id);


--
-- TOC entry 4237 (class 2606 OID 3524158)
-- Name: pk_dataset_type_measurement_type; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dataset_type_measurement_type
    ADD CONSTRAINT pk_dataset_type_measurement_type PRIMARY KEY (dataset_type_id, measurement_metatype_id, measurement_type_id);


--
-- TOC entry 4241 (class 2606 OID 3524160)
-- Name: pk_datatype; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY datatype
    ADD CONSTRAINT pk_datatype PRIMARY KEY (datatype_id);


--
-- TOC entry 4245 (class 2606 OID 3524162)
-- Name: pk_dimension; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dimension
    ADD CONSTRAINT pk_dimension PRIMARY KEY (dimension_id);


--
-- TOC entry 4253 (class 2606 OID 3524164)
-- Name: pk_dimension_domain; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dimension_domain
    ADD CONSTRAINT pk_dimension_domain PRIMARY KEY (domain_id, dimension_id);


--
-- TOC entry 4255 (class 2606 OID 3524166)
-- Name: pk_domain; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY domain
    ADD CONSTRAINT pk_domain PRIMARY KEY (domain_id);


--
-- TOC entry 4261 (class 2606 OID 3524168)
-- Name: pk_indexing_type; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY indexing_type
    ADD CONSTRAINT pk_indexing_type PRIMARY KEY (indexing_type_id);


--
-- TOC entry 4297 (class 2606 OID 3524170)
-- Name: pk_instrument; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY instrument
    ADD CONSTRAINT pk_instrument PRIMARY KEY (instrument_type_id, instrument_id);


--
-- TOC entry 4303 (class 2606 OID 3524172)
-- Name: pk_instrument_type; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY instrument_type
    ADD CONSTRAINT pk_instrument_type PRIMARY KEY (instrument_type_id);


--
-- TOC entry 4307 (class 2606 OID 3524174)
-- Name: pk_measurement_metatype; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY measurement_metatype
    ADD CONSTRAINT pk_measurement_metatype PRIMARY KEY (measurement_metatype_id);


--
-- TOC entry 4312 (class 2606 OID 3524176)
-- Name: pk_measurement_type; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY measurement_type
    ADD CONSTRAINT pk_measurement_type PRIMARY KEY (measurement_metatype_id, measurement_type_id);


--
-- TOC entry 4317 (class 2606 OID 3524178)
-- Name: pk_observation; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY observation
    ADD CONSTRAINT pk_observation PRIMARY KEY (observation_type_id, observation_id);


--
-- TOC entry 4319 (class 2606 OID 3524180)
-- Name: pk_observation_type; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY observation_type
    ADD CONSTRAINT pk_observation_type PRIMARY KEY (observation_type_id);


--
-- TOC entry 4324 (class 2606 OID 3524182)
-- Name: pk_platform; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY platform
    ADD CONSTRAINT pk_platform PRIMARY KEY (platform_type_id, platform_id);


--
-- TOC entry 4330 (class 2606 OID 3524184)
-- Name: pk_platform_type; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY platform_type
    ADD CONSTRAINT pk_platform_type PRIMARY KEY (platform_type_id);


--
-- TOC entry 4265 (class 2606 OID 3524186)
-- Name: pk_reference_system; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY reference_system
    ADD CONSTRAINT pk_reference_system PRIMARY KEY (reference_system_id);


--
-- TOC entry 4271 (class 2606 OID 3524188)
-- Name: pk_reference_system_indexing; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY reference_system_indexing
    ADD CONSTRAINT pk_reference_system_indexing PRIMARY KEY (reference_system_id, array_index);


--
-- TOC entry 4359 (class 2606 OID 3525956)
-- Name: pk_spatial_footprint; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY spatial_footprint
    ADD CONSTRAINT pk_spatial_footprint PRIMARY KEY (spatial_footprint_id);


--
-- TOC entry 4337 (class 2606 OID 3524190)
-- Name: pk_storage; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY storage
    ADD CONSTRAINT pk_storage PRIMARY KEY (storage_type_id, storage_id, storage_version);


--
-- TOC entry 4343 (class 2606 OID 3524192)
-- Name: pk_storage_dataset; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY storage_dataset
    ADD CONSTRAINT pk_storage_dataset PRIMARY KEY (storage_type_id, storage_id, storage_version, dataset_type_id, dataset_id);


--
-- TOC entry 4350 (class 2606 OID 3524194)
-- Name: pk_storage_dimension; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY storage_dimension
    ADD CONSTRAINT pk_storage_dimension PRIMARY KEY (storage_type_id, storage_id, storage_version, domain_id, dimension_id);


--
-- TOC entry 4355 (class 2606 OID 3524196)
-- Name: pk_storage_measurement_type; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY storage_type_measurement_type
    ADD CONSTRAINT pk_storage_measurement_type PRIMARY KEY (storage_type_id, measurement_metatype_id, measurement_type_id);


--
-- TOC entry 4273 (class 2606 OID 3524198)
-- Name: pk_storage_type; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY storage_type
    ADD CONSTRAINT pk_storage_type PRIMARY KEY (storage_type_id);


--
-- TOC entry 4283 (class 2606 OID 3524200)
-- Name: pk_storage_type_dimension; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY storage_type_dimension
    ADD CONSTRAINT pk_storage_type_dimension PRIMARY KEY (storage_type_id, domain_id, dimension_id);


--
-- TOC entry 4293 (class 2606 OID 3524202)
-- Name: pk_storage_type_dimension_property; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY storage_type_dimension_property
    ADD CONSTRAINT pk_storage_type_dimension_property PRIMARY KEY (storage_type_id, domain_id, dimension_id, property_id);


--
-- TOC entry 4287 (class 2606 OID 3524204)
-- Name: property_pkey; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY property
    ADD CONSTRAINT property_pkey PRIMARY KEY (property_id);


--
-- TOC entry 4219 (class 2606 OID 3524206)
-- Name: uq_dataset_type_dataset_type_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dataset_type
    ADD CONSTRAINT uq_dataset_type_dataset_type_name UNIQUE (dataset_type_name);


--
-- TOC entry 4221 (class 2606 OID 3524491)
-- Name: uq_dataset_type_dataset_type_tag; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dataset_type
    ADD CONSTRAINT uq_dataset_type_dataset_type_tag UNIQUE (dataset_type_tag);


--
-- TOC entry 4227 (class 2606 OID 3524208)
-- Name: uq_dataset_type_dimension_dataset_type_id_dimension_order; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dataset_type_dimension
    ADD CONSTRAINT uq_dataset_type_dimension_dataset_type_id_dimension_order UNIQUE (dataset_type_id, dimension_order);


--
-- TOC entry 4239 (class 2606 OID 3524210)
-- Name: uq_dataset_type_measurement_type_dataset_type; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dataset_type_measurement_type
    ADD CONSTRAINT uq_dataset_type_measurement_type_dataset_type UNIQUE (dataset_type_id, measurement_type_index);


--
-- TOC entry 4243 (class 2606 OID 3524212)
-- Name: uq_datatype_datatype_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY datatype
    ADD CONSTRAINT uq_datatype_datatype_name UNIQUE (datatype_name);


--
-- TOC entry 4247 (class 2606 OID 3524214)
-- Name: uq_dimension_dimension_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dimension
    ADD CONSTRAINT uq_dimension_dimension_name UNIQUE (dimension_name);


--
-- TOC entry 4249 (class 2606 OID 3524216)
-- Name: uq_dimension_dimension_tag; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY dimension
    ADD CONSTRAINT uq_dimension_dimension_tag UNIQUE (dimension_tag);


--
-- TOC entry 4257 (class 2606 OID 3524218)
-- Name: uq_domain_domain_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY domain
    ADD CONSTRAINT uq_domain_domain_name UNIQUE (domain_name);


--
-- TOC entry 4259 (class 2606 OID 3524494)
-- Name: uq_domain_domain_tag; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY domain
    ADD CONSTRAINT uq_domain_domain_tag UNIQUE (domain_tag);


--
-- TOC entry 4263 (class 2606 OID 3524220)
-- Name: uq_indexing_type_indexing_type_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY indexing_type
    ADD CONSTRAINT uq_indexing_type_indexing_type_name UNIQUE (indexing_type_name);


--
-- TOC entry 4299 (class 2606 OID 3524222)
-- Name: uq_instrument_instrument_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY instrument
    ADD CONSTRAINT uq_instrument_instrument_name UNIQUE (instrument_name);


--
-- TOC entry 4301 (class 2606 OID 3524496)
-- Name: uq_instrument_instrument_tag; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY instrument
    ADD CONSTRAINT uq_instrument_instrument_tag UNIQUE (instrument_tag);


--
-- TOC entry 4305 (class 2606 OID 3524224)
-- Name: uq_instrument_type_instrument_type_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY instrument_type
    ADD CONSTRAINT uq_instrument_type_instrument_type_name UNIQUE (instrument_type_name);


--
-- TOC entry 4309 (class 2606 OID 3524226)
-- Name: uq_measurement_metatype_measurement_metatype_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY measurement_metatype
    ADD CONSTRAINT uq_measurement_metatype_measurement_metatype_name UNIQUE (measurement_metatype_name);


--
-- TOC entry 4321 (class 2606 OID 3524228)
-- Name: uq_observation_type_observation_type_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY observation_type
    ADD CONSTRAINT uq_observation_type_observation_type_name UNIQUE (observation_type_name);


--
-- TOC entry 4326 (class 2606 OID 3524230)
-- Name: uq_platform_platform_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY platform
    ADD CONSTRAINT uq_platform_platform_name UNIQUE (platform_name);


--
-- TOC entry 4328 (class 2606 OID 3524500)
-- Name: uq_platform_platform_tag; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY platform
    ADD CONSTRAINT uq_platform_platform_tag UNIQUE (platform_tag);


--
-- TOC entry 4332 (class 2606 OID 3524232)
-- Name: uq_platform_type_platform_type_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY platform_type
    ADD CONSTRAINT uq_platform_type_platform_type_name UNIQUE (platform_type_name);


--
-- TOC entry 4289 (class 2606 OID 3524234)
-- Name: uq_property_property_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY property
    ADD CONSTRAINT uq_property_property_name UNIQUE (property_name);


--
-- TOC entry 4267 (class 2606 OID 3524236)
-- Name: uq_reference_system_reference_system_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY reference_system
    ADD CONSTRAINT uq_reference_system_reference_system_name UNIQUE (reference_system_name);


--
-- TOC entry 4339 (class 2606 OID 3524238)
-- Name: uq_storage_storage_location; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY storage
    ADD CONSTRAINT uq_storage_storage_location UNIQUE (storage_location);


--
-- TOC entry 4285 (class 2606 OID 3524240)
-- Name: uq_storage_type_dimension_storage_type_dimension; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY storage_type_dimension
    ADD CONSTRAINT uq_storage_type_dimension_storage_type_dimension UNIQUE (storage_type_id, dimension_id);


--
-- TOC entry 4730 (class 0 OID 0)
-- Dependencies: 4285
-- Name: CONSTRAINT uq_storage_type_dimension_storage_type_dimension ON storage_type_dimension; Type: COMMENT; Schema: public; Owner: cube_admin
--

COMMENT ON CONSTRAINT uq_storage_type_dimension_storage_type_dimension ON storage_type_dimension IS 'Unique constraint to ensure each dimension is only represented once in each storage_type';


--
-- TOC entry 4357 (class 2606 OID 3524242)
-- Name: uq_storage_type_measurement_type_storage_type_id_measurement_ty; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY storage_type_measurement_type
    ADD CONSTRAINT uq_storage_type_measurement_type_storage_type_id_measurement_ty UNIQUE (storage_type_id, measurement_type_index);


--
-- TOC entry 4275 (class 2606 OID 3524244)
-- Name: uq_storage_type_storage_type_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY storage_type
    ADD CONSTRAINT uq_storage_type_storage_type_name UNIQUE (storage_type_name);


--
-- TOC entry 4277 (class 2606 OID 3525972)
-- Name: uq_storage_type_storage_type_tag; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
--

ALTER TABLE ONLY storage_type
    ADD CONSTRAINT uq_storage_type_storage_type_tag UNIQUE (storage_type_tag);


SET search_path = earth_observation, pg_catalog;

--
-- TOC entry 4202 (class 1259 OID 3524245)
-- Name: fki_spectral_parameters_measurement_type; Type: INDEX; Schema: earth_observation; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_spectral_parameters_measurement_type ON spectral_parameters USING btree (measurement_metatype_id, measurement_type_id);


SET search_path = public, pg_catalog;

--
-- TOC entry 4205 (class 1259 OID 3524246)
-- Name: fki_dataset_dataset_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_dataset_type ON dataset USING btree (dataset_type_id);


--
-- TOC entry 4209 (class 1259 OID 3524247)
-- Name: fki_dataset_dimension_dataset; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_dimension_dataset ON dataset_dimension USING btree (dataset_type_id, dataset_id);


--
-- TOC entry 4210 (class 1259 OID 3524248)
-- Name: fki_dataset_dimension_dataset_type_dimension; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_dimension_dataset_type_dimension ON dataset_dimension USING btree (dataset_type_id, domain_id, dimension_id);


--
-- TOC entry 4213 (class 1259 OID 3524249)
-- Name: fki_dataset_metadata_dataset; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_metadata_dataset ON dataset_metadata USING btree (dataset_type_id, dataset_id);


--
-- TOC entry 4206 (class 1259 OID 3524250)
-- Name: fki_dataset_observation; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_observation ON dataset USING btree (observation_type_id, observation_id);


--
-- TOC entry 4222 (class 1259 OID 3524251)
-- Name: fki_dataset_type_dimension_dataset_type_domain; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_type_dimension_dataset_type_domain ON dataset_type_dimension USING btree (dataset_type_id, domain_id);


--
-- TOC entry 4223 (class 1259 OID 3524252)
-- Name: fki_dataset_type_dimension_dimension_domain; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_type_dimension_dimension_domain ON dataset_type_dimension USING btree (domain_id, dimension_id);


--
-- TOC entry 4228 (class 1259 OID 3524253)
-- Name: fki_dataset_type_domain_dataset_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_type_domain_dataset_type ON dataset_type_domain USING btree (dataset_type_id);


--
-- TOC entry 4229 (class 1259 OID 3524254)
-- Name: fki_dataset_type_domain_domain; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_type_domain_domain ON dataset_type_domain USING btree (domain_id);


--
-- TOC entry 4230 (class 1259 OID 3524255)
-- Name: fki_dataset_type_domain_reference_system; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_type_domain_reference_system ON dataset_type_domain USING btree (reference_system_id);


--
-- TOC entry 4233 (class 1259 OID 3524256)
-- Name: fki_dataset_type_measurement_metatype_datatype; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_type_measurement_metatype_datatype ON dataset_type_measurement_type USING btree (datatype_id);


--
-- TOC entry 4234 (class 1259 OID 3524257)
-- Name: fki_dataset_type_measurement_type_dataset_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_type_measurement_type_dataset_type ON dataset_type_measurement_type USING btree (dataset_type_id);


--
-- TOC entry 4235 (class 1259 OID 3524258)
-- Name: fki_dataset_type_measurement_type_measurement_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dataset_type_measurement_type_measurement_type ON dataset_type_measurement_type USING btree (measurement_metatype_id, measurement_type_id);


--
-- TOC entry 4250 (class 1259 OID 3524259)
-- Name: fki_dimension_domain_dimension; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dimension_domain_dimension ON dimension_domain USING btree (dimension_id);


--
-- TOC entry 4251 (class 1259 OID 3524260)
-- Name: fki_dimension_domain_domain; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_dimension_domain_domain ON dimension_domain USING btree (domain_id);


--
-- TOC entry 4294 (class 1259 OID 3524261)
-- Name: fki_instrument_instrument_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_instrument_instrument_type ON instrument USING btree (instrument_type_id);


--
-- TOC entry 4295 (class 1259 OID 3524262)
-- Name: fki_instrument_platform; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_instrument_platform ON instrument USING btree (platform_type_id, platform_id);


--
-- TOC entry 4310 (class 1259 OID 3524263)
-- Name: fki_measurement_type_measurement_metatype; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_measurement_type_measurement_metatype ON measurement_type USING btree (measurement_metatype_id);


--
-- TOC entry 4333 (class 1259 OID 3524264)
-- Name: fki_ndarray_footprint_ndarray_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_ndarray_footprint_ndarray_type ON storage USING btree (storage_type_id);


--
-- TOC entry 4313 (class 1259 OID 3524265)
-- Name: fki_observation_instrument; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_observation_instrument ON observation USING btree (instrument_type_id, instrument_id);


--
-- TOC entry 4314 (class 1259 OID 3524266)
-- Name: fki_observation_observation_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_observation_observation_type ON observation USING btree (observation_type_id);


--
-- TOC entry 4322 (class 1259 OID 3524267)
-- Name: fki_platform_platform_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_platform_platform_type ON platform USING btree (platform_type_id);


--
-- TOC entry 4268 (class 1259 OID 3524268)
-- Name: fki_reference_system_indexing_measurement_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_reference_system_indexing_measurement_type ON reference_system_indexing USING btree (measurement_metatype_id, measurement_type_id);


--
-- TOC entry 4269 (class 1259 OID 3524269)
-- Name: fki_reference_system_indexing_reference_system; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_reference_system_indexing_reference_system ON reference_system_indexing USING btree (reference_system_id);


--
-- TOC entry 4340 (class 1259 OID 3524270)
-- Name: fki_storage_dataset_dataset; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_storage_dataset_dataset ON storage_dataset USING btree (dataset_type_id, dataset_id);


--
-- TOC entry 4341 (class 1259 OID 3524271)
-- Name: fki_storage_dataset_storage; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_storage_dataset_storage ON storage_dataset USING btree (storage_type_id, storage_id, storage_version);


--
-- TOC entry 4344 (class 1259 OID 3524272)
-- Name: fki_storage_dimension_storage; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_storage_dimension_storage ON storage_dimension USING btree (storage_type_id, storage_id, storage_version);


--
-- TOC entry 4345 (class 1259 OID 3524273)
-- Name: fki_storage_dimension_storage_type_dimension; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_storage_dimension_storage_type_dimension ON storage_dimension USING btree (storage_type_id, domain_id, dimension_id);


--
-- TOC entry 4334 (class 1259 OID 3524274)
-- Name: fki_storage_spatial_footprint; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_storage_spatial_footprint ON storage USING btree (spatial_footprint_id);


--
-- TOC entry 4335 (class 1259 OID 3524275)
-- Name: fki_storage_storage_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_storage_storage_type ON storage USING btree (storage_type_id, storage_type_id);


--
-- TOC entry 4290 (class 1259 OID 3524276)
-- Name: fki_storage_type_dimension_attribute_property; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_storage_type_dimension_attribute_property ON storage_type_dimension_property USING btree (property_id);


--
-- TOC entry 4291 (class 1259 OID 3524277)
-- Name: fki_storage_type_dimension_attribute_storage_type_dimension; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_storage_type_dimension_attribute_storage_type_dimension ON storage_type_dimension_property USING btree (storage_type_id, domain_id, dimension_id);


--
-- TOC entry 4278 (class 1259 OID 3524278)
-- Name: fki_storage_type_dimension_dimension_domain; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_storage_type_dimension_dimension_domain ON storage_type_dimension USING btree (domain_id, dimension_id);


--
-- TOC entry 4279 (class 1259 OID 3524279)
-- Name: fki_storage_type_dimension_indexing_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_storage_type_dimension_indexing_type ON storage_type_dimension USING btree (indexing_type_id);


--
-- TOC entry 4280 (class 1259 OID 3525974)
-- Name: fki_storage_type_dimension_reference_system; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_storage_type_dimension_reference_system ON storage_type_dimension USING btree (reference_system_id);


--
-- TOC entry 4281 (class 1259 OID 3525973)
-- Name: fki_storage_type_dimension_storage_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_storage_type_dimension_storage_type ON storage_type_dimension USING btree (storage_type_id);


--
-- TOC entry 4351 (class 1259 OID 3524281)
-- Name: fki_storage_type_masurement_type_datatype; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_storage_type_masurement_type_datatype ON storage_type_measurement_type USING btree (datatype_id);


--
-- TOC entry 4352 (class 1259 OID 3524282)
-- Name: fki_storage_type_measurement_type_measurement_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_storage_type_measurement_type_measurement_type ON storage_type_measurement_type USING btree (measurement_metatype_id, measurement_type_id);


--
-- TOC entry 4353 (class 1259 OID 3524283)
-- Name: fki_storage_type_measurement_type_storage_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX fki_storage_type_measurement_type_storage_type ON storage_type_measurement_type USING btree (storage_type_id);


--
-- TOC entry 4315 (class 1259 OID 3524501)
-- Name: idx_observation_reference; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE UNIQUE INDEX idx_observation_reference ON observation USING btree (observation_reference);


--
-- TOC entry 4346 (class 1259 OID 3524284)
-- Name: idx_storage_dimension_storage_dimension_index; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX idx_storage_dimension_storage_dimension_index ON storage_dimension USING btree (storage_dimension_index);


--
-- TOC entry 4347 (class 1259 OID 3524285)
-- Name: idx_storage_dimension_storage_dimension_max; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX idx_storage_dimension_storage_dimension_max ON storage_dimension USING btree (storage_dimension_max);


--
-- TOC entry 4348 (class 1259 OID 3524286)
-- Name: idx_storage_dimension_storage_dimension_min; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
--

CREATE INDEX idx_storage_dimension_storage_dimension_min ON storage_dimension USING btree (storage_dimension_max);


SET search_path = earth_observation, pg_catalog;

--
-- TOC entry 4360 (class 2606 OID 3524287)
-- Name: fk_spectral_parameters_measurement_metatype; Type: FK CONSTRAINT; Schema: earth_observation; Owner: cube_admin
--

ALTER TABLE ONLY spectral_parameters
    ADD CONSTRAINT fk_spectral_parameters_measurement_metatype FOREIGN KEY (measurement_metatype_id, measurement_type_id) REFERENCES public.measurement_type(measurement_metatype_id, measurement_type_id);


SET search_path = public, pg_catalog;

--
-- TOC entry 4361 (class 2606 OID 3524292)
-- Name: fk_dataset_dataset_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset
    ADD CONSTRAINT fk_dataset_dataset_type FOREIGN KEY (dataset_type_id) REFERENCES dataset_type(dataset_type_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4363 (class 2606 OID 3524297)
-- Name: fk_dataset_dimension_dataset; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset_dimension
    ADD CONSTRAINT fk_dataset_dimension_dataset FOREIGN KEY (dataset_type_id, dataset_id) REFERENCES dataset(dataset_type_id, dataset_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4364 (class 2606 OID 3524302)
-- Name: fk_dataset_dimension_dataset_type_dimension; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset_dimension
    ADD CONSTRAINT fk_dataset_dimension_dataset_type_dimension FOREIGN KEY (dataset_type_id, domain_id, dimension_id) REFERENCES dataset_type_dimension(dataset_type_id, domain_id, dimension_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4365 (class 2606 OID 3524307)
-- Name: fk_dataset_metadata_dataset; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset_metadata
    ADD CONSTRAINT fk_dataset_metadata_dataset FOREIGN KEY (dataset_type_id, dataset_id) REFERENCES dataset(dataset_type_id, dataset_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4362 (class 2606 OID 3524312)
-- Name: fk_dataset_observation; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset
    ADD CONSTRAINT fk_dataset_observation FOREIGN KEY (observation_type_id, observation_id) REFERENCES observation(observation_type_id, observation_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4366 (class 2606 OID 3524317)
-- Name: fk_dataset_type_dimension_dataset_type_domain; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset_type_dimension
    ADD CONSTRAINT fk_dataset_type_dimension_dataset_type_domain FOREIGN KEY (dataset_type_id, domain_id) REFERENCES dataset_type_domain(dataset_type_id, domain_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4367 (class 2606 OID 3524322)
-- Name: fk_dataset_type_dimension_dimension_domain; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset_type_dimension
    ADD CONSTRAINT fk_dataset_type_dimension_dimension_domain FOREIGN KEY (domain_id, dimension_id) REFERENCES dimension_domain(domain_id, dimension_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4368 (class 2606 OID 3524327)
-- Name: fk_dataset_type_domain_dataset_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset_type_domain
    ADD CONSTRAINT fk_dataset_type_domain_dataset_type FOREIGN KEY (dataset_type_id) REFERENCES dataset_type(dataset_type_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4369 (class 2606 OID 3524332)
-- Name: fk_dataset_type_domain_domain; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset_type_domain
    ADD CONSTRAINT fk_dataset_type_domain_domain FOREIGN KEY (domain_id) REFERENCES domain(domain_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4370 (class 2606 OID 3524337)
-- Name: fk_dataset_type_domain_reference_system; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset_type_domain
    ADD CONSTRAINT fk_dataset_type_domain_reference_system FOREIGN KEY (reference_system_id) REFERENCES reference_system(reference_system_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4371 (class 2606 OID 3524342)
-- Name: fk_dataset_type_measurement_type_dataset_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset_type_measurement_type
    ADD CONSTRAINT fk_dataset_type_measurement_type_dataset_type FOREIGN KEY (dataset_type_id) REFERENCES dataset_type(dataset_type_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4372 (class 2606 OID 3524347)
-- Name: fk_dataset_type_measurement_type_datatype; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset_type_measurement_type
    ADD CONSTRAINT fk_dataset_type_measurement_type_datatype FOREIGN KEY (datatype_id) REFERENCES datatype(datatype_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4373 (class 2606 OID 3524352)
-- Name: fk_dataset_type_measurement_type_measurement_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dataset_type_measurement_type
    ADD CONSTRAINT fk_dataset_type_measurement_type_measurement_type FOREIGN KEY (measurement_metatype_id, measurement_type_id) REFERENCES measurement_type(measurement_metatype_id, measurement_type_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4374 (class 2606 OID 3524357)
-- Name: fk_dimension_domain_dimension; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dimension_domain
    ADD CONSTRAINT fk_dimension_domain_dimension FOREIGN KEY (dimension_id) REFERENCES dimension(dimension_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4375 (class 2606 OID 3524362)
-- Name: fk_dimension_domain_domain; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY dimension_domain
    ADD CONSTRAINT fk_dimension_domain_domain FOREIGN KEY (domain_id) REFERENCES domain(domain_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4384 (class 2606 OID 3524367)
-- Name: fk_instrument_instrument_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY instrument
    ADD CONSTRAINT fk_instrument_instrument_type FOREIGN KEY (instrument_type_id) REFERENCES instrument_type(instrument_type_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4385 (class 2606 OID 3524372)
-- Name: fk_instrument_platform; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY instrument
    ADD CONSTRAINT fk_instrument_platform FOREIGN KEY (platform_type_id, platform_id) REFERENCES platform(platform_type_id, platform_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4386 (class 2606 OID 3524377)
-- Name: fk_measurement_type_measurement_metatype; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY measurement_type
    ADD CONSTRAINT fk_measurement_type_measurement_metatype FOREIGN KEY (measurement_metatype_id) REFERENCES measurement_metatype(measurement_metatype_id);


--
-- TOC entry 4387 (class 2606 OID 3524382)
-- Name: fk_observation_instrument; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY observation
    ADD CONSTRAINT fk_observation_instrument FOREIGN KEY (instrument_type_id, instrument_id) REFERENCES instrument(instrument_type_id, instrument_id);


--
-- TOC entry 4388 (class 2606 OID 3524387)
-- Name: fk_observation_observation_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY observation
    ADD CONSTRAINT fk_observation_observation_type FOREIGN KEY (observation_type_id) REFERENCES observation_type(observation_type_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4389 (class 2606 OID 3524392)
-- Name: fk_platform_platform_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY platform
    ADD CONSTRAINT fk_platform_platform_type FOREIGN KEY (platform_type_id) REFERENCES platform_type(platform_type_id);


--
-- TOC entry 4376 (class 2606 OID 3524397)
-- Name: fk_reference_system_indexing_measurement_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY reference_system_indexing
    ADD CONSTRAINT fk_reference_system_indexing_measurement_type FOREIGN KEY (measurement_metatype_id, measurement_type_id) REFERENCES measurement_type(measurement_metatype_id, measurement_type_id);


--
-- TOC entry 4377 (class 2606 OID 3524402)
-- Name: fk_reference_system_indexing_reference_system; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY reference_system_indexing
    ADD CONSTRAINT fk_reference_system_indexing_reference_system FOREIGN KEY (reference_system_id) REFERENCES reference_system(reference_system_id);


--
-- TOC entry 4391 (class 2606 OID 3525957)
-- Name: fk_spatial_footprint; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY storage
    ADD CONSTRAINT fk_spatial_footprint FOREIGN KEY (spatial_footprint_id) REFERENCES spatial_footprint(spatial_footprint_id);


--
-- TOC entry 4392 (class 2606 OID 3524407)
-- Name: fk_storage_dataset_dataset; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY storage_dataset
    ADD CONSTRAINT fk_storage_dataset_dataset FOREIGN KEY (dataset_type_id, dataset_id) REFERENCES dataset(dataset_type_id, dataset_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4393 (class 2606 OID 3524412)
-- Name: fk_storage_dataset_storage; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY storage_dataset
    ADD CONSTRAINT fk_storage_dataset_storage FOREIGN KEY (storage_type_id, storage_id, storage_version) REFERENCES storage(storage_type_id, storage_id, storage_version) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4394 (class 2606 OID 3524417)
-- Name: fk_storage_dimension_storage; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY storage_dimension
    ADD CONSTRAINT fk_storage_dimension_storage FOREIGN KEY (storage_type_id, storage_id, storage_version) REFERENCES storage(storage_type_id, storage_id, storage_version) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4395 (class 2606 OID 3524422)
-- Name: fk_storage_dimension_storage_type_dimension; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY storage_dimension
    ADD CONSTRAINT fk_storage_dimension_storage_type_dimension FOREIGN KEY (storage_type_id, domain_id, dimension_id) REFERENCES storage_type_dimension(storage_type_id, domain_id, dimension_id);


--
-- TOC entry 4390 (class 2606 OID 3524427)
-- Name: fk_storage_storage_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY storage
    ADD CONSTRAINT fk_storage_storage_type FOREIGN KEY (storage_type_id) REFERENCES storage_type(storage_type_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4382 (class 2606 OID 3524432)
-- Name: fk_storage_type_dimension_attribute_storage_type_dimension; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY storage_type_dimension_property
    ADD CONSTRAINT fk_storage_type_dimension_attribute_storage_type_dimension FOREIGN KEY (storage_type_id, domain_id, dimension_id) REFERENCES storage_type_dimension(storage_type_id, domain_id, dimension_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4378 (class 2606 OID 3524437)
-- Name: fk_storage_type_dimension_dimension_domain; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY storage_type_dimension
    ADD CONSTRAINT fk_storage_type_dimension_dimension_domain FOREIGN KEY (domain_id, dimension_id) REFERENCES dimension_domain(domain_id, dimension_id) ON UPDATE CASCADE;


--
-- TOC entry 4379 (class 2606 OID 3524442)
-- Name: fk_storage_type_dimension_indexing_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY storage_type_dimension
    ADD CONSTRAINT fk_storage_type_dimension_indexing_type FOREIGN KEY (indexing_type_id) REFERENCES indexing_type(indexing_type_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4383 (class 2606 OID 3524447)
-- Name: fk_storage_type_dimension_property_property; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY storage_type_dimension_property
    ADD CONSTRAINT fk_storage_type_dimension_property_property FOREIGN KEY (property_id) REFERENCES property(property_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4380 (class 2606 OID 3524452)
-- Name: fk_storage_type_dimension_reference_system; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY storage_type_dimension
    ADD CONSTRAINT fk_storage_type_dimension_reference_system FOREIGN KEY (reference_system_id) REFERENCES reference_system(reference_system_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4381 (class 2606 OID 3524457)
-- Name: fk_storage_type_dimension_storage_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY storage_type_dimension
    ADD CONSTRAINT fk_storage_type_dimension_storage_type FOREIGN KEY (storage_type_id) REFERENCES storage_type(storage_type_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4396 (class 2606 OID 3524462)
-- Name: fk_storage_type_measurement_type_datatype; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY storage_type_measurement_type
    ADD CONSTRAINT fk_storage_type_measurement_type_datatype FOREIGN KEY (datatype_id) REFERENCES datatype(datatype_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4397 (class 2606 OID 3524467)
-- Name: fk_storage_type_measurement_type_measurement_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY storage_type_measurement_type
    ADD CONSTRAINT fk_storage_type_measurement_type_measurement_type FOREIGN KEY (measurement_metatype_id, measurement_type_id) REFERENCES measurement_type(measurement_metatype_id, measurement_type_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4398 (class 2606 OID 3524472)
-- Name: fk_storage_type_measurement_type_storage_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
--

ALTER TABLE ONLY storage_type_measurement_type
    ADD CONSTRAINT fk_storage_type_measurement_type_storage_type FOREIGN KEY (storage_type_id) REFERENCES storage_type(storage_type_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 4521 (class 0 OID 0)
-- Dependencies: 8
-- Name: public; Type: ACL; Schema: -; Owner: postgres
--

REVOKE ALL ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM postgres;
GRANT ALL ON SCHEMA public TO postgres;
GRANT ALL ON SCHEMA public TO cube_admin;
GRANT ALL ON SCHEMA public TO PUBLIC;


--
-- TOC entry 4523 (class 0 OID 0)
-- Dependencies: 9
-- Name: ztmp; Type: ACL; Schema: -; Owner: postgres
--

REVOKE ALL ON SCHEMA ztmp FROM PUBLIC;
REVOKE ALL ON SCHEMA ztmp FROM postgres;
GRANT ALL ON SCHEMA ztmp TO postgres;
GRANT ALL ON SCHEMA ztmp TO PUBLIC;


SET search_path = earth_observation, pg_catalog;

--
-- TOC entry 4525 (class 0 OID 0)
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
-- TOC entry 4535 (class 0 OID 0)
-- Dependencies: 177
-- Name: dataset; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE dataset FROM PUBLIC;
REVOKE ALL ON TABLE dataset FROM cube_admin;
GRANT ALL ON TABLE dataset TO cube_admin;
GRANT ALL ON TABLE dataset TO cube_admin_group;
GRANT SELECT ON TABLE dataset TO cube_user_group;


--
-- TOC entry 4544 (class 0 OID 0)
-- Dependencies: 178
-- Name: dataset_dimension; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE dataset_dimension FROM PUBLIC;
REVOKE ALL ON TABLE dataset_dimension FROM cube_admin;
GRANT ALL ON TABLE dataset_dimension TO cube_admin;
GRANT ALL ON TABLE dataset_dimension TO cube_admin_group;
GRANT SELECT ON TABLE dataset_dimension TO cube_user_group;


--
-- TOC entry 4549 (class 0 OID 0)
-- Dependencies: 180
-- Name: dataset_metadata; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE dataset_metadata FROM PUBLIC;
REVOKE ALL ON TABLE dataset_metadata FROM cube_admin;
GRANT ALL ON TABLE dataset_metadata TO cube_admin;
GRANT ALL ON TABLE dataset_metadata TO cube_admin_group;
GRANT SELECT ON TABLE dataset_metadata TO cube_user_group;


--
-- TOC entry 4554 (class 0 OID 0)
-- Dependencies: 181
-- Name: dataset_type; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE dataset_type FROM PUBLIC;
REVOKE ALL ON TABLE dataset_type FROM cube_admin;
GRANT ALL ON TABLE dataset_type TO cube_admin;
GRANT ALL ON TABLE dataset_type TO cube_admin_group;
GRANT SELECT ON TABLE dataset_type TO cube_user_group;


--
-- TOC entry 4561 (class 0 OID 0)
-- Dependencies: 182
-- Name: dataset_type_dimension; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE dataset_type_dimension FROM PUBLIC;
REVOKE ALL ON TABLE dataset_type_dimension FROM cube_admin;
GRANT ALL ON TABLE dataset_type_dimension TO cube_admin;
GRANT ALL ON TABLE dataset_type_dimension TO cube_admin_group;
GRANT SELECT ON TABLE dataset_type_dimension TO cube_user_group;


--
-- TOC entry 4566 (class 0 OID 0)
-- Dependencies: 183
-- Name: dataset_type_domain; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE dataset_type_domain FROM PUBLIC;
REVOKE ALL ON TABLE dataset_type_domain FROM cube_admin;
GRANT ALL ON TABLE dataset_type_domain TO cube_admin;
GRANT ALL ON TABLE dataset_type_domain TO cube_admin_group;
GRANT SELECT ON TABLE dataset_type_domain TO cube_user_group;


--
-- TOC entry 4573 (class 0 OID 0)
-- Dependencies: 184
-- Name: dataset_type_measurement_type; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE dataset_type_measurement_type FROM PUBLIC;
REVOKE ALL ON TABLE dataset_type_measurement_type FROM cube_admin;
GRANT ALL ON TABLE dataset_type_measurement_type TO cube_admin;
GRANT ALL ON TABLE dataset_type_measurement_type TO cube_admin_group;
GRANT SELECT ON TABLE dataset_type_measurement_type TO cube_user_group;


--
-- TOC entry 4580 (class 0 OID 0)
-- Dependencies: 185
-- Name: datatype; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE datatype FROM PUBLIC;
REVOKE ALL ON TABLE datatype FROM cube_admin;
GRANT ALL ON TABLE datatype TO cube_admin;
GRANT ALL ON TABLE datatype TO cube_admin_group;
GRANT SELECT ON TABLE datatype TO cube_user_group;


--
-- TOC entry 4585 (class 0 OID 0)
-- Dependencies: 186
-- Name: dimension; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE dimension FROM PUBLIC;
REVOKE ALL ON TABLE dimension FROM cube_admin;
GRANT ALL ON TABLE dimension TO cube_admin;
GRANT ALL ON TABLE dimension TO cube_admin_group;
GRANT SELECT ON TABLE dimension TO cube_user_group;


--
-- TOC entry 4589 (class 0 OID 0)
-- Dependencies: 187
-- Name: dimension_domain; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE dimension_domain FROM PUBLIC;
REVOKE ALL ON TABLE dimension_domain FROM cube_admin;
GRANT ALL ON TABLE dimension_domain TO cube_admin;
GRANT ALL ON TABLE dimension_domain TO cube_admin_group;
GRANT SELECT ON TABLE dimension_domain TO cube_user_group;


--
-- TOC entry 4594 (class 0 OID 0)
-- Dependencies: 188
-- Name: domain; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE domain FROM PUBLIC;
REVOKE ALL ON TABLE domain FROM cube_admin;
GRANT ALL ON TABLE domain TO cube_admin;
GRANT ALL ON TABLE domain TO cube_admin_group;
GRANT SELECT ON TABLE domain TO cube_user_group;


--
-- TOC entry 4598 (class 0 OID 0)
-- Dependencies: 189
-- Name: indexing_type; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE indexing_type FROM PUBLIC;
REVOKE ALL ON TABLE indexing_type FROM cube_admin;
GRANT ALL ON TABLE indexing_type TO cube_admin;
GRANT ALL ON TABLE indexing_type TO cube_admin_group;
GRANT SELECT ON TABLE indexing_type TO cube_user_group;


--
-- TOC entry 4605 (class 0 OID 0)
-- Dependencies: 190
-- Name: reference_system; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE reference_system FROM PUBLIC;
REVOKE ALL ON TABLE reference_system FROM cube_admin;
GRANT ALL ON TABLE reference_system TO cube_admin;
GRANT ALL ON TABLE reference_system TO cube_admin_group;
GRANT SELECT ON TABLE reference_system TO cube_user_group;


--
-- TOC entry 4612 (class 0 OID 0)
-- Dependencies: 191
-- Name: reference_system_indexing; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE reference_system_indexing FROM PUBLIC;
REVOKE ALL ON TABLE reference_system_indexing FROM cube_admin;
GRANT ALL ON TABLE reference_system_indexing TO cube_admin;
GRANT ALL ON TABLE reference_system_indexing TO cube_admin_group;
GRANT SELECT ON TABLE reference_system_indexing TO cube_user_group;


--
-- TOC entry 4618 (class 0 OID 0)
-- Dependencies: 192
-- Name: storage_type; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE storage_type FROM PUBLIC;
REVOKE ALL ON TABLE storage_type FROM cube_admin;
GRANT ALL ON TABLE storage_type TO cube_admin;
GRANT ALL ON TABLE storage_type TO cube_admin_group;
GRANT SELECT ON TABLE storage_type TO cube_user_group;


--
-- TOC entry 4632 (class 0 OID 0)
-- Dependencies: 193
-- Name: storage_type_dimension; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE storage_type_dimension FROM PUBLIC;
REVOKE ALL ON TABLE storage_type_dimension FROM cube_admin;
GRANT ALL ON TABLE storage_type_dimension TO cube_admin;
GRANT ALL ON TABLE storage_type_dimension TO cube_admin_group;
GRANT SELECT ON TABLE storage_type_dimension TO cube_user_group;


--
-- TOC entry 4633 (class 0 OID 0)
-- Dependencies: 194
-- Name: storage_type_dimension_view; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE storage_type_dimension_view FROM PUBLIC;
REVOKE ALL ON TABLE storage_type_dimension_view FROM cube_admin;
GRANT ALL ON TABLE storage_type_dimension_view TO cube_admin;
GRANT ALL ON TABLE storage_type_dimension_view TO cube_admin_group;
GRANT SELECT ON TABLE storage_type_dimension_view TO cube_user_group;


--
-- TOC entry 4634 (class 0 OID 0)
-- Dependencies: 195
-- Name: dimension_indices_view; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE dimension_indices_view FROM PUBLIC;
REVOKE ALL ON TABLE dimension_indices_view FROM cube_admin;
GRANT ALL ON TABLE dimension_indices_view TO cube_admin;
GRANT ALL ON TABLE dimension_indices_view TO cube_admin_group;
GRANT SELECT ON TABLE dimension_indices_view TO cube_user_group;


--
-- TOC entry 4639 (class 0 OID 0)
-- Dependencies: 196
-- Name: property; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE property FROM PUBLIC;
REVOKE ALL ON TABLE property FROM cube_admin;
GRANT ALL ON TABLE property TO cube_admin;
GRANT ALL ON TABLE property TO cube_admin_group;
GRANT SELECT ON TABLE property TO cube_user_group;


--
-- TOC entry 4646 (class 0 OID 0)
-- Dependencies: 197
-- Name: storage_type_dimension_property; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE storage_type_dimension_property FROM PUBLIC;
REVOKE ALL ON TABLE storage_type_dimension_property FROM cube_admin;
GRANT ALL ON TABLE storage_type_dimension_property TO cube_admin;
GRANT ALL ON TABLE storage_type_dimension_property TO cube_admin_group;
GRANT SELECT ON TABLE storage_type_dimension_property TO cube_user_group;


--
-- TOC entry 4647 (class 0 OID 0)
-- Dependencies: 198
-- Name: dimension_properties_view; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE dimension_properties_view FROM PUBLIC;
REVOKE ALL ON TABLE dimension_properties_view FROM cube_admin;
GRANT ALL ON TABLE dimension_properties_view TO cube_admin;
GRANT ALL ON TABLE dimension_properties_view TO cube_admin_group;
GRANT SELECT ON TABLE dimension_properties_view TO cube_user_group;


--
-- TOC entry 4655 (class 0 OID 0)
-- Dependencies: 199
-- Name: instrument; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE instrument FROM PUBLIC;
REVOKE ALL ON TABLE instrument FROM cube_admin;
GRANT ALL ON TABLE instrument TO cube_admin;
GRANT ALL ON TABLE instrument TO cube_admin_group;
GRANT SELECT ON TABLE instrument TO cube_user_group;


--
-- TOC entry 4659 (class 0 OID 0)
-- Dependencies: 200
-- Name: instrument_type; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE instrument_type FROM PUBLIC;
REVOKE ALL ON TABLE instrument_type FROM cube_admin;
GRANT ALL ON TABLE instrument_type TO cube_admin;
GRANT ALL ON TABLE instrument_type TO cube_admin_group;
GRANT SELECT ON TABLE instrument_type TO cube_user_group;


--
-- TOC entry 4663 (class 0 OID 0)
-- Dependencies: 201
-- Name: measurement_metatype; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE measurement_metatype FROM PUBLIC;
REVOKE ALL ON TABLE measurement_metatype FROM cube_admin;
GRANT ALL ON TABLE measurement_metatype TO cube_admin;
GRANT ALL ON TABLE measurement_metatype TO cube_admin_group;
GRANT SELECT ON TABLE measurement_metatype TO cube_user_group;


--
-- TOC entry 4669 (class 0 OID 0)
-- Dependencies: 202
-- Name: measurement_type; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE measurement_type FROM PUBLIC;
REVOKE ALL ON TABLE measurement_type FROM cube_admin;
GRANT ALL ON TABLE measurement_type TO cube_admin;
GRANT ALL ON TABLE measurement_type TO cube_admin_group;
GRANT SELECT ON TABLE measurement_type TO cube_user_group;


--
-- TOC entry 4678 (class 0 OID 0)
-- Dependencies: 203
-- Name: observation; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE observation FROM PUBLIC;
REVOKE ALL ON TABLE observation FROM cube_admin;
GRANT ALL ON TABLE observation TO cube_admin;
GRANT ALL ON TABLE observation TO cube_admin_group;
GRANT SELECT ON TABLE observation TO cube_user_group;


--
-- TOC entry 4682 (class 0 OID 0)
-- Dependencies: 205
-- Name: observation_type; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE observation_type FROM PUBLIC;
REVOKE ALL ON TABLE observation_type FROM cube_admin;
GRANT ALL ON TABLE observation_type TO cube_admin;
GRANT ALL ON TABLE observation_type TO cube_admin_group;
GRANT SELECT ON TABLE observation_type TO cube_user_group;


--
-- TOC entry 4688 (class 0 OID 0)
-- Dependencies: 206
-- Name: platform; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE platform FROM PUBLIC;
REVOKE ALL ON TABLE platform FROM cube_admin;
GRANT ALL ON TABLE platform TO cube_admin;
GRANT ALL ON TABLE platform TO cube_admin_group;
GRANT SELECT ON TABLE platform TO cube_user_group;


--
-- TOC entry 4692 (class 0 OID 0)
-- Dependencies: 207
-- Name: platform_type; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE platform_type FROM PUBLIC;
REVOKE ALL ON TABLE platform_type FROM cube_admin;
GRANT ALL ON TABLE platform_type TO cube_admin;
GRANT ALL ON TABLE platform_type TO cube_admin_group;
GRANT SELECT ON TABLE platform_type TO cube_user_group;


--
-- TOC entry 4704 (class 0 OID 0)
-- Dependencies: 208
-- Name: storage; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE storage FROM PUBLIC;
REVOKE ALL ON TABLE storage FROM cube_admin;
GRANT ALL ON TABLE storage TO cube_admin;
GRANT ALL ON TABLE storage TO cube_admin_group;
GRANT SELECT ON TABLE storage TO cube_user_group;


--
-- TOC entry 4711 (class 0 OID 0)
-- Dependencies: 209
-- Name: storage_dataset; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE storage_dataset FROM PUBLIC;
REVOKE ALL ON TABLE storage_dataset FROM cube_admin;
GRANT ALL ON TABLE storage_dataset TO cube_admin;
GRANT ALL ON TABLE storage_dataset TO cube_admin_group;
GRANT SELECT ON TABLE storage_dataset TO cube_user_group;


--
-- TOC entry 4721 (class 0 OID 0)
-- Dependencies: 210
-- Name: storage_dimension; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE storage_dimension FROM PUBLIC;
REVOKE ALL ON TABLE storage_dimension FROM cube_admin;
GRANT ALL ON TABLE storage_dimension TO cube_admin;
GRANT ALL ON TABLE storage_dimension TO cube_admin_group;
GRANT SELECT ON TABLE storage_dimension TO cube_user_group;


--
-- TOC entry 4729 (class 0 OID 0)
-- Dependencies: 212
-- Name: storage_type_measurement_type; Type: ACL; Schema: public; Owner: cube_admin
--

REVOKE ALL ON TABLE storage_type_measurement_type FROM PUBLIC;
REVOKE ALL ON TABLE storage_type_measurement_type FROM cube_admin;
GRANT ALL ON TABLE storage_type_measurement_type TO cube_admin;
GRANT ALL ON TABLE storage_type_measurement_type TO cube_admin_group;
GRANT SELECT ON TABLE storage_type_measurement_type TO cube_user_group;


-- Completed on 2015-07-22 17:38:08

--
-- PostgreSQL database dump complete
--

