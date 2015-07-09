--
-- PostgreSQL database dump
--

-- Dumped from database version 9.3.7
-- Dumped by pg_dump version 9.3.1
-- Started on 2015-07-09 13:27:08

SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;

SET search_path = public, pg_catalog;

--
-- TOC entry 3050 (class 0 OID 3506105)
-- Dependencies: 180
-- Data for Name: dataset_type; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO dataset_type VALUES (2, 'NBAR', 'NBAR');
INSERT INTO dataset_type VALUES (3, 'PQ', 'PQ');
INSERT INTO dataset_type VALUES (4, 'FC', 'FC');
INSERT INTO dataset_type VALUES (1, 'ORTHO', 'ORTHO');
INSERT INTO dataset_type VALUES (5, 'L1T', 'L1T');
INSERT INTO dataset_type VALUES (20, 'MOD09', 'MOD09');
INSERT INTO dataset_type VALUES (22, 'RBQ500', 'RBQ500');


--
-- TOC entry 3057 (class 0 OID 3506126)
-- Dependencies: 187
-- Data for Name: domain; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO domain VALUES (1, 'Spatial XY', 'XY');
INSERT INTO domain VALUES (2, 'Spatial Z', 'Z');
INSERT INTO domain VALUES (3, 'Temporal', 'T');
INSERT INTO domain VALUES (4, 'Spectral', 'B');
INSERT INTO domain VALUES (5, 'Spatial XYZ', 'XYZ');


--
-- TOC entry 3059 (class 0 OID 3506132)
-- Dependencies: 189
-- Data for Name: reference_system; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO reference_system VALUES (3, 'Australian Height Datum (AHD)', 'metres', 'AHD', 'AHD');
INSERT INTO reference_system VALUES (50, 'Landsat 5 Reflectance Bands', 'band', 'LS5', 'LS5');
INSERT INTO reference_system VALUES (70, 'Landsat 7 Spectral Bands', 'band', 'LS7', 'LS7');
INSERT INTO reference_system VALUES (80, 'Landsat 8 Band', 'band', 'LS8', 'LS8');
INSERT INTO reference_system VALUES (4326, 'Unprojected WGS84 (Lat-long)', 'degrees', 'EPSG:4326', 'EPSG:4326');
INSERT INTO reference_system VALUES (5, 'Year', 'year', 'YEAR', 'YEAR');
INSERT INTO reference_system VALUES (4, 'Seconds since 1/1/1970 0:00', 'seconds', 'seconds since 1970-01-01 00:00:00', 'SSE');


--
-- TOC entry 3052 (class 0 OID 3506111)
-- Dependencies: 182
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
-- TOC entry 3055 (class 0 OID 3506120)
-- Dependencies: 185
-- Data for Name: dimension; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO dimension VALUES (1, 'longitude', 'X');
INSERT INTO dimension VALUES (2, 'latitude', 'Y');
INSERT INTO dimension VALUES (3, 'height/depth', 'Z');
INSERT INTO dimension VALUES (5, 'spectral', 'LAMBDA');
INSERT INTO dimension VALUES (4, 'time', 'T');


--
-- TOC entry 3056 (class 0 OID 3506123)
-- Dependencies: 186
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
-- TOC entry 3051 (class 0 OID 3506108)
-- Dependencies: 181
-- Data for Name: dataset_type_dimension; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO dataset_type_dimension VALUES (1, 1, 1, 3);
INSERT INTO dataset_type_dimension VALUES (1, 1, 2, 2);
INSERT INTO dataset_type_dimension VALUES (1, 3, 4, 1);
INSERT INTO dataset_type_dimension VALUES (2, 1, 1, 3);
INSERT INTO dataset_type_dimension VALUES (2, 1, 2, 2);
INSERT INTO dataset_type_dimension VALUES (2, 3, 4, 1);
INSERT INTO dataset_type_dimension VALUES (3, 1, 1, 3);
INSERT INTO dataset_type_dimension VALUES (3, 1, 2, 2);
INSERT INTO dataset_type_dimension VALUES (3, 3, 4, 1);
INSERT INTO dataset_type_dimension VALUES (4, 1, 1, 3);
INSERT INTO dataset_type_dimension VALUES (4, 1, 2, 2);
INSERT INTO dataset_type_dimension VALUES (4, 3, 4, 1);
INSERT INTO dataset_type_dimension VALUES (5, 1, 1, 3);
INSERT INTO dataset_type_dimension VALUES (5, 1, 2, 2);
INSERT INTO dataset_type_dimension VALUES (5, 3, 4, 1);
INSERT INTO dataset_type_dimension VALUES (20, 1, 1, 3);
INSERT INTO dataset_type_dimension VALUES (20, 1, 2, 2);
INSERT INTO dataset_type_dimension VALUES (20, 3, 4, 1);
INSERT INTO dataset_type_dimension VALUES (22, 1, 1, 3);
INSERT INTO dataset_type_dimension VALUES (22, 1, 2, 2);
INSERT INTO dataset_type_dimension VALUES (22, 3, 4, 1);


--
-- TOC entry 3054 (class 0 OID 3506117)
-- Dependencies: 184
-- Data for Name: datatype; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO datatype VALUES (3, 'string', NULL, NULL, NULL);
INSERT INTO datatype VALUES (1, 'int16', 'int16', 'int16', 'i2');
INSERT INTO datatype VALUES (2, 'float32', 'float32', 'float32', 'f4');


--
-- TOC entry 3067 (class 0 OID 3506176)
-- Dependencies: 200
-- Data for Name: measurement_metatype; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO measurement_metatype VALUES (10, 'Multi-band Spectral Radiance');
INSERT INTO measurement_metatype VALUES (1, 'Spectral Radiance (Single Band)');
INSERT INTO measurement_metatype VALUES (3, 'Categorical Pixel Quality Bit-array Integer');


--
-- TOC entry 3068 (class 0 OID 3506179)
-- Dependencies: 201
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
INSERT INTO measurement_type VALUES (1, 200, 'MODIS Surface Reflectance Band 1', 'MB1');
INSERT INTO measurement_type VALUES (3, 3, 'Landsat Pixel Quality Bit-Array', 'PQ');
INSERT INTO measurement_type VALUES (1, 201, 'MODIS Surface Reflectance Band 2', 'MB2');
INSERT INTO measurement_type VALUES (1, 202, 'MODIS Surface Reflectance Band 3', 'MB3');
INSERT INTO measurement_type VALUES (1, 203, 'MODIS Surface Reflectance Band 4', 'MB4');
INSERT INTO measurement_type VALUES (1, 204, 'MODIS Surface Reflectance Band 5', 'MB5');
INSERT INTO measurement_type VALUES (1, 205, 'MODIS Surface Reflectance Band 6', 'MB6');
INSERT INTO measurement_type VALUES (1, 206, 'MODIS Surface Reflectance Band 7', 'MB7');
INSERT INTO measurement_type VALUES (1, 207, 'MODIS Surface Reflectance Band 8', 'MB8');
INSERT INTO measurement_type VALUES (1, 208, 'MODIS Surface Reflectance Band 9', 'MB9');
INSERT INTO measurement_type VALUES (1, 209, 'MODIS Surface Reflectance Band 10', 'MB10');
INSERT INTO measurement_type VALUES (1, 210, 'MODIS Surface Reflectance Band 11', 'MB11');
INSERT INTO measurement_type VALUES (1, 211, 'MODIS Surface Reflectance Band 12', 'MB12');
INSERT INTO measurement_type VALUES (1, 212, 'MODIS Surface Reflectance Band 13', 'MB13');
INSERT INTO measurement_type VALUES (1, 213, 'MODIS Surface Reflectance Band 14', 'MB14');
INSERT INTO measurement_type VALUES (1, 214, 'MODIS Surface Reflectance Band 15', 'MB15');
INSERT INTO measurement_type VALUES (1, 215, 'MODIS Surface Reflectance Band 16', 'MB16');
INSERT INTO measurement_type VALUES (1, 216, 'MODIS Surface Reflectance Band 17', 'MB17');
INSERT INTO measurement_type VALUES (1, 217, 'MODIS Surface Reflectance Band 18', 'MB18');
INSERT INTO measurement_type VALUES (1, 218, 'MODIS Surface Reflectance Band 19', 'MB19');
INSERT INTO measurement_type VALUES (1, 225, 'MODIS Surface Reflectance Band 26', 'MB26');
INSERT INTO measurement_type VALUES (1, 219, 'MODIS Surface Reflectance Band 20', 'MB20');
INSERT INTO measurement_type VALUES (1, 220, 'MODIS Surface Reflectance Band 21', 'MB21');
INSERT INTO measurement_type VALUES (1, 221, 'MODIS Surface Reflectance Band 22', 'MB22');
INSERT INTO measurement_type VALUES (1, 222, 'MODIS Surface Reflectance Band 23', 'MB23');
INSERT INTO measurement_type VALUES (1, 223, 'MODIS Surface Reflectance Band 24', 'MB24');
INSERT INTO measurement_type VALUES (1, 224, 'MODIS Surface Reflectance Band 25', 'MB25');
INSERT INTO measurement_type VALUES (1, 226, 'MODIS Surface Reflectance Band 27', 'MB27');
INSERT INTO measurement_type VALUES (1, 227, 'MODIS Surface Reflectance Band 28', 'MB28');
INSERT INTO measurement_type VALUES (1, 228, 'MODIS Surface Reflectance Band 29', 'MB29');
INSERT INTO measurement_type VALUES (1, 229, 'MODIS Surface Reflectance Band 30', 'MB30');
INSERT INTO measurement_type VALUES (1, 230, 'MODIS Surface Reflectance Band 31', 'MB31');
INSERT INTO measurement_type VALUES (1, 231, 'MODIS Surface Reflectance Band 32', 'MB32');
INSERT INTO measurement_type VALUES (1, 232, 'MODIS Surface Reflectance Band 33', 'MB33');
INSERT INTO measurement_type VALUES (1, 233, 'MODIS Surface Reflectance Band 34', 'MB34');
INSERT INTO measurement_type VALUES (1, 234, 'MODIS Surface Reflectance Band 35', 'MB35');
INSERT INTO measurement_type VALUES (1, 235, 'MODIS Surface Reflectance Band 36', 'MB36');
INSERT INTO measurement_type VALUES (1, 250, '500m MODIS Reflectance Band Quality', 'RBQ500');
INSERT INTO measurement_type VALUES (1, 260, '1km MODIS Reflectance Band Quality', 'RBQ1000');


--
-- TOC entry 3053 (class 0 OID 3506114)
-- Dependencies: 183
-- Data for Name: dataset_type_measurement_type; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO dataset_type_measurement_type VALUES (3, 3, 3, 1, 1);


--
-- TOC entry 3058 (class 0 OID 3506129)
-- Dependencies: 188
-- Data for Name: indexing_type; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO indexing_type VALUES (1, 'regular');
INSERT INTO indexing_type VALUES (2, 'irregular');
INSERT INTO indexing_type VALUES (3, 'fixed');


--
-- TOC entry 3066 (class 0 OID 3506173)
-- Dependencies: 199
-- Data for Name: instrument_type; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO instrument_type VALUES (1, 'Passive Satellite-borne');


--
-- TOC entry 3071 (class 0 OID 3506193)
-- Dependencies: 205
-- Data for Name: platform_type; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO platform_type VALUES (1, 'Satellite');


--
-- TOC entry 3070 (class 0 OID 3506190)
-- Dependencies: 204
-- Data for Name: platform; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO platform VALUES (1, 5, 'Landsat 5');
INSERT INTO platform VALUES (1, 7, 'Landsat 7');
INSERT INTO platform VALUES (1, 8, 'Landsat 8');
INSERT INTO platform VALUES (1, 100, 'MODIS Terra');


--
-- TOC entry 3065 (class 0 OID 3506170)
-- Dependencies: 198
-- Data for Name: instrument; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO instrument VALUES (1, 5, 'Thematic Mapper', 1, 5, 'TM');
INSERT INTO instrument VALUES (1, 81, 'Operational Land Imager', 1, 8, 'OLI');
INSERT INTO instrument VALUES (1, 82, 'Thermal Infrared Sensor', 1, 8, 'TIRS');
INSERT INTO instrument VALUES (1, 8, 'Operational Land Imager / Thermal Infrared Sensor', 1, 8, 'OLI_TIRS');
INSERT INTO instrument VALUES (1, 7, 'Enhanced Thematic Mapper Plus', 1, 7, 'ETM+');
INSERT INTO instrument VALUES (1, 100, 'MODIS-Terra', 1, 100, 'MODIS-Terra');


--
-- TOC entry 3069 (class 0 OID 3506187)
-- Dependencies: 203
-- Data for Name: observation_type; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO observation_type VALUES (1, 'Optical Satellite');


--
-- TOC entry 3063 (class 0 OID 3506159)
-- Dependencies: 195
-- Data for Name: property; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO property VALUES (4, 'axis', 3);
INSERT INTO property VALUES (2, 'standard_name', 3);
INSERT INTO property VALUES (1, 'long_name', 3);
INSERT INTO property VALUES (3, 'units', 3);
INSERT INTO property VALUES (5, 'calendar', 3);


--
-- TOC entry 3060 (class 0 OID 3506135)
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
-- TOC entry 3061 (class 0 OID 3506138)
-- Dependencies: 191
-- Data for Name: storage_type; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO storage_type VALUES (5, 'Landsat 5 TM ARG-25', 'LS5TM', '/g/data2/rs0/gdf_trial/20150709');
INSERT INTO storage_type VALUES (7, 'Landsat 7 ETM ARG-25', 'LS7ETM', '/g/data2/rs0/gdf_trial/20150709');
INSERT INTO storage_type VALUES (82, 'Landsat 8 TIRS', 'LS8TIRS', '/g/data2/rs0/gdf_trial/20150709');
INSERT INTO storage_type VALUES (50, 'Landsat 5 TM ARG-25 with spectral dimension', 'LS5TM-SD', '/g/data2/rs0/gdf_trial/20150709');
INSERT INTO storage_type VALUES (8, 'Landsat 8 OLI ARG-25', 'LS8OLI', '/g/data2/rs0/gdf_trial/20150709');
INSERT INTO storage_type VALUES (51, 'Landsat 5 TM ARG-25 Pixel Quality', 'LS5TMPQ', '/g/data2/rs0/gdf_trial/20150709');
INSERT INTO storage_type VALUES (71, 'Landsat 7 ETM ARG-25 Pixel Quality', 'LS7ETMPQ', '/g/data2/rs0/gdf_trial/20150709');
INSERT INTO storage_type VALUES (81, 'Landsat 8 OLI ARG-25 Pixel Quality', 'LS8OLIPQ', '/g/data2/rs0/gdf_trial/20150709');
INSERT INTO storage_type VALUES (100, 'MODIS Terra MOD09', 'MOD09', '/g/data2/rs0/gdf_trial/20150709');
INSERT INTO storage_type VALUES (101, 'MODIS Terra RBQ500', 'MOD09RBQ', '/g/data2/rs0/gdf_trial/20150709');


--
-- TOC entry 3062 (class 0 OID 3506144)
-- Dependencies: 192
-- Data for Name: storage_type_dimension; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO storage_type_dimension VALUES (51, 3, 4, 1, 1, 31622400, 128, 0, 2, 4, 5, false);
INSERT INTO storage_type_dimension VALUES (71, 3, 4, 1, 1, 31622400, 128, 0, 2, 4, 5, false);
INSERT INTO storage_type_dimension VALUES (81, 3, 4, 1, 1, 31622400, 128, 0, 2, 4, 5, false);
INSERT INTO storage_type_dimension VALUES (100, 3, 4, 1, 1, 31622400, 128, 0, 2, 4, 5, false);
INSERT INTO storage_type_dimension VALUES (101, 3, 4, 1, 1, 31622400, 128, 0, 2, 4, 5, false);
INSERT INTO storage_type_dimension VALUES (5, 3, 4, 1, 1, 31622400, 128, 0, 2, 4, 5, false);
INSERT INTO storage_type_dimension VALUES (50, 3, 4, 2, 1, 31622400, 128, 0, 2, 4, 5, false);
INSERT INTO storage_type_dimension VALUES (7, 3, 4, 1, 1, 31622400, 128, 0, 2, 4, 5, false);
INSERT INTO storage_type_dimension VALUES (8, 3, 4, 1, 1, 31622400, 128, 0, 2, 4, 5, false);
INSERT INTO storage_type_dimension VALUES (50, 4, 5, 1, NULL, NULL, NULL, NULL, 3, 50, 50, false);
INSERT INTO storage_type_dimension VALUES (5, 1, 1, 3, 1, 4000, 128, 0, 1, 4326, 4326, false);
INSERT INTO storage_type_dimension VALUES (7, 1, 1, 3, 1, 4000, 128, 0, 1, 4326, 4326, false);
INSERT INTO storage_type_dimension VALUES (8, 1, 1, 3, 1, 4000, 128, 0, 1, 4326, 4326, false);
INSERT INTO storage_type_dimension VALUES (51, 1, 1, 3, 1, 4000, 128, 0, 1, 4326, 4326, false);
INSERT INTO storage_type_dimension VALUES (71, 1, 1, 3, 1, 4000, 128, 0, 1, 4326, 4326, false);
INSERT INTO storage_type_dimension VALUES (81, 1, 1, 3, 1, 4000, 128, 0, 1, 4326, 4326, false);
INSERT INTO storage_type_dimension VALUES (100, 1, 1, 3, 10, 2000, 128, 0, 1, 4326, 4326, false);
INSERT INTO storage_type_dimension VALUES (101, 1, 1, 3, 10, 4000, 128, 0, 1, 4326, 4326, false);
INSERT INTO storage_type_dimension VALUES (71, 1, 2, 2, 1, 4000, 128, 0, 1, 4326, 4326, true);
INSERT INTO storage_type_dimension VALUES (81, 1, 2, 2, 1, 4000, 128, 0, 1, 4326, 4326, true);
INSERT INTO storage_type_dimension VALUES (51, 1, 2, 2, 1, 4000, 128, 0, 1, 4326, 4326, true);
INSERT INTO storage_type_dimension VALUES (101, 1, 2, 2, 10, 4000, 128, 0, 1, 4326, 4326, true);
INSERT INTO storage_type_dimension VALUES (7, 1, 2, 2, 1, 4000, 128, 0, 1, 4326, 4326, true);
INSERT INTO storage_type_dimension VALUES (8, 1, 2, 2, 1, 4000, 128, 0, 1, 4326, 4326, true);
INSERT INTO storage_type_dimension VALUES (5, 1, 2, 2, 1, 4000, 128, 0, 1, 4326, 4326, true);
INSERT INTO storage_type_dimension VALUES (100, 1, 2, 2, 10, 2000, 128, 0, 1, 4326, 4326, true);
INSERT INTO storage_type_dimension VALUES (50, 1, 1, 4, 1, 4000, 128, 0, 1, 4326, 4326, false);
INSERT INTO storage_type_dimension VALUES (50, 1, 2, 3, 1, 4000, 128, 0, 1, 4326, 4326, true);


--
-- TOC entry 3064 (class 0 OID 3506162)
-- Dependencies: 196
-- Data for Name: storage_type_dimension_property; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO storage_type_dimension_property VALUES (5, 1, 1, 1, 'longitude');
INSERT INTO storage_type_dimension_property VALUES (5, 1, 1, 2, 'longitude');
INSERT INTO storage_type_dimension_property VALUES (5, 1, 1, 3, 'degrees_east');
INSERT INTO storage_type_dimension_property VALUES (5, 1, 1, 4, 'X');
INSERT INTO storage_type_dimension_property VALUES (5, 3, 4, 5, 'gregorian');
INSERT INTO storage_type_dimension_property VALUES (5, 1, 2, 1, 'latitude');
INSERT INTO storage_type_dimension_property VALUES (5, 1, 2, 2, 'latitude');
INSERT INTO storage_type_dimension_property VALUES (5, 1, 2, 3, 'degrees_north');
INSERT INTO storage_type_dimension_property VALUES (5, 1, 2, 4, 'Y');
INSERT INTO storage_type_dimension_property VALUES (5, 3, 4, 1, 'time');
INSERT INTO storage_type_dimension_property VALUES (5, 3, 4, 2, 'time');
INSERT INTO storage_type_dimension_property VALUES (5, 3, 4, 3, 'seconds since 1970-01-01 ');
INSERT INTO storage_type_dimension_property VALUES (5, 3, 4, 4, 'T');
INSERT INTO storage_type_dimension_property VALUES (8, 1, 1, 1, 'longitude');
INSERT INTO storage_type_dimension_property VALUES (8, 1, 1, 2, 'longitude');
INSERT INTO storage_type_dimension_property VALUES (8, 1, 1, 3, 'degrees_east');
INSERT INTO storage_type_dimension_property VALUES (8, 1, 1, 4, 'X');
INSERT INTO storage_type_dimension_property VALUES (8, 3, 4, 5, 'gregorian');
INSERT INTO storage_type_dimension_property VALUES (8, 1, 2, 1, 'latitude');
INSERT INTO storage_type_dimension_property VALUES (8, 1, 2, 2, 'latitude');
INSERT INTO storage_type_dimension_property VALUES (8, 1, 2, 3, 'degrees_north');
INSERT INTO storage_type_dimension_property VALUES (8, 1, 2, 4, 'Y');
INSERT INTO storage_type_dimension_property VALUES (8, 3, 4, 1, 'time');
INSERT INTO storage_type_dimension_property VALUES (8, 3, 4, 2, 'time');
INSERT INTO storage_type_dimension_property VALUES (8, 3, 4, 3, 'seconds since 1970-01-01 ');
INSERT INTO storage_type_dimension_property VALUES (8, 3, 4, 4, 'T');
INSERT INTO storage_type_dimension_property VALUES (7, 1, 1, 1, 'longitude');
INSERT INTO storage_type_dimension_property VALUES (7, 1, 1, 2, 'longitude');
INSERT INTO storage_type_dimension_property VALUES (7, 1, 1, 3, 'degrees_east');
INSERT INTO storage_type_dimension_property VALUES (7, 1, 1, 4, 'X');
INSERT INTO storage_type_dimension_property VALUES (7, 3, 4, 5, 'gregorian');
INSERT INTO storage_type_dimension_property VALUES (7, 1, 2, 1, 'latitude');
INSERT INTO storage_type_dimension_property VALUES (7, 1, 2, 2, 'latitude');
INSERT INTO storage_type_dimension_property VALUES (7, 1, 2, 3, 'degrees_north');
INSERT INTO storage_type_dimension_property VALUES (7, 1, 2, 4, 'Y');
INSERT INTO storage_type_dimension_property VALUES (7, 3, 4, 1, 'time');
INSERT INTO storage_type_dimension_property VALUES (7, 3, 4, 2, 'time');
INSERT INTO storage_type_dimension_property VALUES (7, 3, 4, 3, 'seconds since 1970-01-01 ');
INSERT INTO storage_type_dimension_property VALUES (7, 3, 4, 4, 'T');
INSERT INTO storage_type_dimension_property VALUES (51, 1, 1, 1, 'longitude');
INSERT INTO storage_type_dimension_property VALUES (51, 1, 1, 2, 'longitude');
INSERT INTO storage_type_dimension_property VALUES (51, 1, 1, 3, 'degrees_east');
INSERT INTO storage_type_dimension_property VALUES (51, 1, 1, 4, 'X');
INSERT INTO storage_type_dimension_property VALUES (51, 3, 4, 5, 'gregorian');
INSERT INTO storage_type_dimension_property VALUES (51, 1, 2, 1, 'latitude');
INSERT INTO storage_type_dimension_property VALUES (51, 1, 2, 2, 'latitude');
INSERT INTO storage_type_dimension_property VALUES (51, 1, 2, 3, 'degrees_north');
INSERT INTO storage_type_dimension_property VALUES (51, 1, 2, 4, 'Y');
INSERT INTO storage_type_dimension_property VALUES (51, 3, 4, 1, 'time');
INSERT INTO storage_type_dimension_property VALUES (51, 3, 4, 2, 'time');
INSERT INTO storage_type_dimension_property VALUES (51, 3, 4, 3, 'seconds since 1970-01-01 ');
INSERT INTO storage_type_dimension_property VALUES (51, 3, 4, 4, 'T');
INSERT INTO storage_type_dimension_property VALUES (81, 1, 1, 1, 'longitude');
INSERT INTO storage_type_dimension_property VALUES (81, 1, 1, 2, 'longitude');
INSERT INTO storage_type_dimension_property VALUES (81, 1, 1, 3, 'degrees_east');
INSERT INTO storage_type_dimension_property VALUES (81, 1, 1, 4, 'X');
INSERT INTO storage_type_dimension_property VALUES (81, 3, 4, 5, 'gregorian');
INSERT INTO storage_type_dimension_property VALUES (81, 1, 2, 1, 'latitude');
INSERT INTO storage_type_dimension_property VALUES (81, 1, 2, 2, 'latitude');
INSERT INTO storage_type_dimension_property VALUES (81, 1, 2, 3, 'degrees_north');
INSERT INTO storage_type_dimension_property VALUES (81, 1, 2, 4, 'Y');
INSERT INTO storage_type_dimension_property VALUES (81, 3, 4, 1, 'time');
INSERT INTO storage_type_dimension_property VALUES (81, 3, 4, 2, 'time');
INSERT INTO storage_type_dimension_property VALUES (81, 3, 4, 3, 'seconds since 1970-01-01 ');
INSERT INTO storage_type_dimension_property VALUES (81, 3, 4, 4, 'T');
INSERT INTO storage_type_dimension_property VALUES (71, 1, 1, 1, 'longitude');
INSERT INTO storage_type_dimension_property VALUES (71, 1, 1, 2, 'longitude');
INSERT INTO storage_type_dimension_property VALUES (71, 1, 1, 3, 'degrees_east');
INSERT INTO storage_type_dimension_property VALUES (71, 1, 1, 4, 'X');
INSERT INTO storage_type_dimension_property VALUES (71, 3, 4, 5, 'gregorian');
INSERT INTO storage_type_dimension_property VALUES (71, 1, 2, 1, 'latitude');
INSERT INTO storage_type_dimension_property VALUES (71, 1, 2, 2, 'latitude');
INSERT INTO storage_type_dimension_property VALUES (71, 1, 2, 3, 'degrees_north');
INSERT INTO storage_type_dimension_property VALUES (71, 1, 2, 4, 'Y');
INSERT INTO storage_type_dimension_property VALUES (71, 3, 4, 1, 'time');
INSERT INTO storage_type_dimension_property VALUES (71, 3, 4, 2, 'time');
INSERT INTO storage_type_dimension_property VALUES (71, 3, 4, 3, 'seconds since 1970-01-01 ');
INSERT INTO storage_type_dimension_property VALUES (71, 3, 4, 4, 'T');
INSERT INTO storage_type_dimension_property VALUES (100, 1, 1, 1, 'longitude');
INSERT INTO storage_type_dimension_property VALUES (100, 1, 1, 2, 'longitude');
INSERT INTO storage_type_dimension_property VALUES (100, 1, 1, 3, 'degrees_east');
INSERT INTO storage_type_dimension_property VALUES (100, 1, 1, 4, 'X');
INSERT INTO storage_type_dimension_property VALUES (100, 3, 4, 5, 'gregorian');
INSERT INTO storage_type_dimension_property VALUES (100, 1, 2, 1, 'latitude');
INSERT INTO storage_type_dimension_property VALUES (100, 1, 2, 2, 'latitude');
INSERT INTO storage_type_dimension_property VALUES (100, 1, 2, 3, 'degrees_north');
INSERT INTO storage_type_dimension_property VALUES (100, 1, 2, 4, 'Y');
INSERT INTO storage_type_dimension_property VALUES (100, 3, 4, 1, 'time');
INSERT INTO storage_type_dimension_property VALUES (100, 3, 4, 2, 'time');
INSERT INTO storage_type_dimension_property VALUES (100, 3, 4, 3, 'seconds since 1970-01-01 ');
INSERT INTO storage_type_dimension_property VALUES (100, 3, 4, 4, 'T');


--
-- TOC entry 3072 (class 0 OID 3506207)
-- Dependencies: 209
-- Data for Name: storage_type_measurement_type; Type: TABLE DATA; Schema: public; Owner: cube_admin
--

INSERT INTO storage_type_measurement_type VALUES (5, 1, 51, 1, 1, -999);
INSERT INTO storage_type_measurement_type VALUES (5, 1, 52, 1, 2, -999);
INSERT INTO storage_type_measurement_type VALUES (5, 1, 53, 1, 3, -999);
INSERT INTO storage_type_measurement_type VALUES (5, 1, 54, 1, 4, -999);
INSERT INTO storage_type_measurement_type VALUES (5, 1, 55, 1, 5, -999);
INSERT INTO storage_type_measurement_type VALUES (5, 1, 57, 1, 6, -999);
INSERT INTO storage_type_measurement_type VALUES (7, 1, 71, 1, 1, -999);
INSERT INTO storage_type_measurement_type VALUES (7, 1, 72, 1, 2, -999);
INSERT INTO storage_type_measurement_type VALUES (7, 1, 73, 1, 3, -999);
INSERT INTO storage_type_measurement_type VALUES (7, 1, 74, 1, 4, -999);
INSERT INTO storage_type_measurement_type VALUES (7, 1, 75, 1, 5, -999);
INSERT INTO storage_type_measurement_type VALUES (7, 1, 77, 1, 6, -999);
INSERT INTO storage_type_measurement_type VALUES (50, 10, 1000, 1, 1, -999);
INSERT INTO storage_type_measurement_type VALUES (51, 3, 3, 1, 1, NULL);
INSERT INTO storage_type_measurement_type VALUES (71, 3, 3, 1, 1, NULL);
INSERT INTO storage_type_measurement_type VALUES (81, 3, 3, 1, 1, NULL);
INSERT INTO storage_type_measurement_type VALUES (100, 1, 200, 1, 1, -28672);
INSERT INTO storage_type_measurement_type VALUES (100, 1, 201, 1, 2, -28672);
INSERT INTO storage_type_measurement_type VALUES (100, 1, 202, 1, 3, -28672);
INSERT INTO storage_type_measurement_type VALUES (100, 1, 203, 1, 4, -28672);
INSERT INTO storage_type_measurement_type VALUES (8, 1, 81, 1, 1, -999);
INSERT INTO storage_type_measurement_type VALUES (8, 1, 82, 1, 2, -999);
INSERT INTO storage_type_measurement_type VALUES (8, 1, 84, 1, 4, -999);
INSERT INTO storage_type_measurement_type VALUES (8, 1, 83, 1, 3, -999);
INSERT INTO storage_type_measurement_type VALUES (8, 1, 85, 1, 5, -999);
INSERT INTO storage_type_measurement_type VALUES (8, 1, 86, 1, 6, -999);
INSERT INTO storage_type_measurement_type VALUES (8, 1, 87, 1, 7, -999);
INSERT INTO storage_type_measurement_type VALUES (100, 1, 204, 1, 5, -28672);
INSERT INTO storage_type_measurement_type VALUES (100, 1, 205, 1, 6, -28672);
INSERT INTO storage_type_measurement_type VALUES (100, 1, 206, 1, 7, -28672);


-- Completed on 2015-07-09 13:27:35

--
-- PostgreSQL database dump complete
--

