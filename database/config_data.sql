--
-- PostgreSQL database dump
--

-- Dumped from database version 9.3.6
-- Dumped by pg_dump version 9.3.6
-- Started on 2015-04-09 10:29:20 AEST

SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;

SET search_path = public, pg_catalog;

--
-- TOC entry 4472 (class 0 OID 2485987)
-- Dependencies: 228
-- Data for Name: dataset_type; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO dataset_type VALUES (2, 'NBAR');
INSERT INTO dataset_type VALUES (3, 'PQ');
INSERT INTO dataset_type VALUES (4, 'FC');
INSERT INTO dataset_type VALUES (1, 'ORTHO');
INSERT INTO dataset_type VALUES (5, 'L1T');
INSERT INTO dataset_type VALUES (20, 'MOD09');
INSERT INTO dataset_type VALUES (22, 'RBQ500');


--
-- TOC entry 4479 (class 0 OID 2486008)
-- Dependencies: 235
-- Data for Name: domain; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO domain VALUES (1, 'Spatial XY', 'XY');
INSERT INTO domain VALUES (2, 'Spatial Z', 'Z');
INSERT INTO domain VALUES (3, 'Temporal', 'T');
INSERT INTO domain VALUES (4, 'Spectral', 'B');
INSERT INTO domain VALUES (5, 'Spatial XYZ', 'XYZ');


--
-- TOC entry 4483 (class 0 OID 2486020)
-- Dependencies: 239
-- Data for Name: reference_system; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO reference_system VALUES (3, 'Australian Height Datum (AHD)', 'metres', 'AHD', 'AHD');
INSERT INTO reference_system VALUES (4, 'Seconds since 1/1/1970 0:00', 'seconds', 'SSE', 'SSE');
INSERT INTO reference_system VALUES (50, 'Landsat 5 Reflectance Bands', 'band', 'LS5', 'LS5');
INSERT INTO reference_system VALUES (70, 'Landsat 7 Spectral Bands', 'band', 'LS7', 'LS7');
INSERT INTO reference_system VALUES (80, 'Landsat 8 Band', 'band', 'LS8', 'LS8');
INSERT INTO reference_system VALUES (4326, 'Unprojected WGS84 (Lat-long)', 'degrees', 'EPSG:4326', 'EPSG:4326');
INSERT INTO reference_system VALUES (5, 'Year', 'year', 'YEAR', 'YEAR');


--
-- TOC entry 4474 (class 0 OID 2485993)
-- Dependencies: 230
-- Data for Name: dataset_type_domain; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO dataset_type_domain VALUES (1, 1, 4326);
INSERT INTO dataset_type_domain VALUES (2, 1, 4326);
INSERT INTO dataset_type_domain VALUES (3, 1, 4326);
INSERT INTO dataset_type_domain VALUES (4, 1, 4326);
INSERT INTO dataset_type_domain VALUES (5, 1, 4326);
INSERT INTO dataset_type_domain VALUES (1, 3, 4);
INSERT INTO dataset_type_domain VALUES (2, 3, 4);
INSERT INTO dataset_type_domain VALUES (3, 3, 4);
INSERT INTO dataset_type_domain VALUES (4, 3, 4);
INSERT INTO dataset_type_domain VALUES (5, 3, 4);
INSERT INTO dataset_type_domain VALUES (20, 1, 4326);
INSERT INTO dataset_type_domain VALUES (20, 3, 4);
INSERT INTO dataset_type_domain VALUES (22, 1, 4326);
INSERT INTO dataset_type_domain VALUES (22, 3, 4);


--
-- TOC entry 4477 (class 0 OID 2486002)
-- Dependencies: 233
-- Data for Name: dimension; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO dimension VALUES (1, 'longitude', 'X');
INSERT INTO dimension VALUES (2, 'latitude', 'Y');
INSERT INTO dimension VALUES (3, 'height/depth', 'Z');
INSERT INTO dimension VALUES (5, 'spectral', 'LAMBDA');
INSERT INTO dimension VALUES (4, 'time', 'T');


--
-- TOC entry 4478 (class 0 OID 2486005)
-- Dependencies: 234
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
-- TOC entry 4473 (class 0 OID 2485990)
-- Dependencies: 229
-- Data for Name: dataset_type_dimension; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO dataset_type_dimension VALUES (1, 1, 1);
INSERT INTO dataset_type_dimension VALUES (1, 1, 2);
INSERT INTO dataset_type_dimension VALUES (2, 1, 1);
INSERT INTO dataset_type_dimension VALUES (2, 1, 2);
INSERT INTO dataset_type_dimension VALUES (3, 1, 1);
INSERT INTO dataset_type_dimension VALUES (3, 1, 2);
INSERT INTO dataset_type_dimension VALUES (4, 1, 1);
INSERT INTO dataset_type_dimension VALUES (4, 1, 2);
INSERT INTO dataset_type_dimension VALUES (5, 1, 1);
INSERT INTO dataset_type_dimension VALUES (5, 1, 2);
INSERT INTO dataset_type_dimension VALUES (1, 3, 4);
INSERT INTO dataset_type_dimension VALUES (2, 3, 4);
INSERT INTO dataset_type_dimension VALUES (3, 3, 4);
INSERT INTO dataset_type_dimension VALUES (4, 3, 4);
INSERT INTO dataset_type_dimension VALUES (5, 3, 4);
INSERT INTO dataset_type_dimension VALUES (20, 1, 2);
INSERT INTO dataset_type_dimension VALUES (20, 1, 1);
INSERT INTO dataset_type_dimension VALUES (20, 3, 4);
INSERT INTO dataset_type_dimension VALUES (22, 1, 2);
INSERT INTO dataset_type_dimension VALUES (22, 1, 1);
INSERT INTO dataset_type_dimension VALUES (22, 3, 4);


--
-- TOC entry 4476 (class 0 OID 2485999)
-- Dependencies: 232
-- Data for Name: datatype; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO datatype VALUES (3, 'string', NULL, NULL, NULL);
INSERT INTO datatype VALUES (1, 'int16', 'int16', 'int16', 'i2');
INSERT INTO datatype VALUES (2, 'float32', 'float32', 'float32', 'f4');


--
-- TOC entry 4489 (class 0 OID 2486055)
-- Dependencies: 248
-- Data for Name: measurement_metatype; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO measurement_metatype VALUES (10, 'Multi-band Spectral Radiance');
INSERT INTO measurement_metatype VALUES (1, 'Spectral Radiance (Single Band)');


--
-- TOC entry 4490 (class 0 OID 2486058)
-- Dependencies: 249
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
-- TOC entry 4475 (class 0 OID 2485996)
-- Dependencies: 231
-- Data for Name: dataset_type_measurement_type; Type: TABLE DATA; Schema: public; Owner: cube_admin
--



--
-- TOC entry 4480 (class 0 OID 2486011)
-- Dependencies: 236
-- Data for Name: indexing_type; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO indexing_type VALUES (1, 'regular');
INSERT INTO indexing_type VALUES (2, 'irregular');
INSERT INTO indexing_type VALUES (3, 'fixed');


--
-- TOC entry 4488 (class 0 OID 2486052)
-- Dependencies: 247
-- Data for Name: instrument_type; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO instrument_type VALUES (1, 'Passive Satellite-borne');


--
-- TOC entry 4494 (class 0 OID 2486093)
-- Dependencies: 260
-- Data for Name: platform_type; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO platform_type VALUES (1, 'Satellite');


--
-- TOC entry 4493 (class 0 OID 2486090)
-- Dependencies: 259
-- Data for Name: platform; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO platform VALUES (1, 5, 'Landsat 5');
INSERT INTO platform VALUES (1, 7, 'Landsat 7');
INSERT INTO platform VALUES (1, 8, 'Landsat 8');
INSERT INTO platform VALUES (1, 100, 'MODIS Terra');


--
-- TOC entry 4487 (class 0 OID 2486049)
-- Dependencies: 246
-- Data for Name: instrument; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO instrument VALUES (1, 5, 'Thematic Mapper', 1, 5);
INSERT INTO instrument VALUES (1, 7, 'Enhanced Thematic Mapper Plus', 1, 7);
INSERT INTO instrument VALUES (1, 8, 'Operational Land Imager / Thermal Infrared Sensor', 1, 8);
INSERT INTO instrument VALUES (1, 81, 'Operational Land Imager', 1, 8);
INSERT INTO instrument VALUES (1, 82, 'Thermal Infrared Sensor', 1, 8);
INSERT INTO instrument VALUES (1, 100, 'MODIS-Terra', 1, 100);


--
-- TOC entry 4481 (class 0 OID 2486014)
-- Dependencies: 237
-- Data for Name: ndarray_type; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO ndarray_type VALUES (5, 'Landsat 5 TM ARG-25', 'LS5TM');
INSERT INTO ndarray_type VALUES (7, 'Landsat 7 ETM ARG-25', 'LS7ETM');
INSERT INTO ndarray_type VALUES (82, 'Landsat 8 TIRS', 'LS8TIRS');
INSERT INTO ndarray_type VALUES (50, 'Landsat 5 TM ARG-25 with spectral dimension', 'LS5TM-SD');
INSERT INTO ndarray_type VALUES (8, 'Landsat 8 OLI ARG-25', 'LS8OLI');
INSERT INTO ndarray_type VALUES (100, 'MODIS Terra MOD09', 'MODIS-TERRA-M09');
INSERT INTO ndarray_type VALUES (101, 'MODIS Terra RBQ500', 'MODIS-TERRA-R500');


--
-- TOC entry 4482 (class 0 OID 2486017)
-- Dependencies: 238
-- Data for Name: ndarray_type_dimension; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO ndarray_type_dimension VALUES (5, 1, 1, 2, 1, 4000, 128, 0, 1, 4326, 4326);
INSERT INTO ndarray_type_dimension VALUES (5, 1, 2, 3, 1, 4000, 128, 0, 1, 4326, 4326);
INSERT INTO ndarray_type_dimension VALUES (50, 1, 1, 3, 1, 4000, 128, 0, 1, 4326, 4326);
INSERT INTO ndarray_type_dimension VALUES (50, 1, 2, 4, 1, 4000, 128, 0, 1, 4326, 4326);
INSERT INTO ndarray_type_dimension VALUES (50, 4, 5, 1, NULL, NULL, NULL, NULL, 3, 50, 50);
INSERT INTO ndarray_type_dimension VALUES (5, 3, 4, 1, 1, 31622400, 128, 0, 2, 4, 5);
INSERT INTO ndarray_type_dimension VALUES (50, 3, 4, 2, 1, 31622400, 128, 0, 2, 4, 5);
INSERT INTO ndarray_type_dimension VALUES (7, 1, 1, 2, 1, 4000, 128, 0, 1, 4326, 4326);
INSERT INTO ndarray_type_dimension VALUES (7, 1, 2, 3, 1, 4000, 128, 0, 1, 4326, 4326);
INSERT INTO ndarray_type_dimension VALUES (7, 3, 4, 1, 1, 31622400, 128, 0, 2, 4, 5);
INSERT INTO ndarray_type_dimension VALUES (8, 1, 1, 2, 1, 4000, 128, 0, 1, 4326, 4326);
INSERT INTO ndarray_type_dimension VALUES (8, 1, 2, 3, 1, 4000, 128, 0, 1, 4326, 4326);
INSERT INTO ndarray_type_dimension VALUES (8, 3, 4, 1, 1, 31622400, 128, 0, 2, 4, 5);
INSERT INTO ndarray_type_dimension VALUES (100, 3, 4, 1, 1, 31622400, 128, 0, 2, 4, 5);
INSERT INTO ndarray_type_dimension VALUES (100, 1, 1, 2, 10, 4000, 128, 0, 1, 4326, 4326);
INSERT INTO ndarray_type_dimension VALUES (100, 1, 2, 3, 10, 4000, 128, 0, 1, 4326, 4326);
INSERT INTO ndarray_type_dimension VALUES (101, 1, 1, 2, 10, 4000, 128, 0, 1, 4326, 4326);
INSERT INTO ndarray_type_dimension VALUES (101, 1, 2, 3, 10, 4000, 128, 0, 1, 4326, 4326);
INSERT INTO ndarray_type_dimension VALUES (101, 3, 4, 1, 1, 31622400, 128, 0, 2, 4, 5);


--
-- TOC entry 4486 (class 0 OID 2486041)
-- Dependencies: 244
-- Data for Name: property; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO property VALUES (4, 'axis', 3);
INSERT INTO property VALUES (2, 'standard_name', 3);
INSERT INTO property VALUES (1, 'long_name', 3);
INSERT INTO property VALUES (3, 'units', 3);
INSERT INTO property VALUES (5, 'calendar', 3);


--
-- TOC entry 4485 (class 0 OID 2486038)
-- Dependencies: 243
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
-- TOC entry 4491 (class 0 OID 2486081)
-- Dependencies: 256
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
-- TOC entry 4492 (class 0 OID 2486087)
-- Dependencies: 258
-- Data for Name: observation_type; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO observation_type VALUES (1, 'Optical Satellite');


--
-- TOC entry 4484 (class 0 OID 2486031)
-- Dependencies: 241
-- Data for Name: reference_system_indexing; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO reference_system_indexing VALUES (50, 0, 'Band 1 - Visible Blue', 1, 51);
INSERT INTO reference_system_indexing VALUES (50, 1, 'Band 2 - Visible Green', 1, 52);
INSERT INTO reference_system_indexing VALUES (50, 2, 'Band 3 - Visible Red', 1, 53);
INSERT INTO reference_system_indexing VALUES (50, 3, 'Band 4 - Near Infrared', 1, 54);
INSERT INTO reference_system_indexing VALUES (50, 4, 'Band 5 - Middle Infrared 1', 1, 55);
INSERT INTO reference_system_indexing VALUES (50, 5, 'Band 7 - Middle Infrared 2', 1, 57);


--
-- TOC entry 4221 (class 0 OID 2484784)
-- Dependencies: 203
-- Data for Name: spatial_ref_sys; Type: TABLE DATA; Schema: public; Owner: cube_admin
--



-- Completed on 2015-04-09 10:29:49 AEST

--
-- PostgreSQL database dump complete
--

