-- Name: ndarray_type; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
ALTER TABLE ndarray_type RENAME TO storage_type;
ALTER TABLE storage_type RENAME COLUMN ndarray_type_id TO storage_type_id;
ALTER TABLE storage_type RENAME COLUMN ndarray_type_name TO storage_type_name;
ALTER TABLE storage_type RENAME COLUMN ndarray_type_tag TO storage_type_tag;

COMMENT ON TABLE storage_type IS 'Configuration: storage parameter lookup table. Used TO manage different storage_types';

-- Name: ndarray_type_dimension; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
ALTER TABLE ndarray_type_dimension RENAME TO storage_type_dimension;
ALTER TABLE storage_type_dimension RENAME COLUMN ndarray_type_id TO storage_type_id;
ALTER TABLE storage_type_dimension RENAME COLUMN creation_order TO dimension_order;
COMMENT ON TABLE storage_type_dimension IS 'Configuration: Association between storage type and dimensions. Used TO define dimensionality of storage type';

-- Name: ndarray_type_dimension_property; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
ALTER TABLE ndarray_type_dimension_property RENAME TO storage_type_dimension_property;
ALTER TABLE storage_type_dimension_property RENAME COLUMN ndarray_type_id TO storage_type_id;
COMMENT ON TABLE storage_type_dimension_property IS 'Configuration: Metadata properties of dimension in storage type';

-- Name: ndarray; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
ALTER TABLE ndarray RENAME TO storage;
ALTER TABLE storage RENAME COLUMN ndarray_type_id TO storage_type_id;
ALTER TABLE storage RENAME COLUMN ndarray_id TO storage_id;
ALTER TABLE storage RENAME COLUMN ndarray_version TO storage_version;
ALTER TABLE storage RENAME COLUMN ndarray_location TO storage_location;
ALTER TABLE storage RENAME COLUMN ndarray_bytes TO storage_bytes;
COMMENT ON TABLE storage IS 'Data: n-dimensional data structure instances';

-- Name: ndarray_dataset; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
ALTER TABLE ndarray_dataset RENAME TO storage_dataset;
ALTER TABLE storage_dataset RENAME COLUMN ndarray_type_id TO storage_type_id;
ALTER TABLE storage_dataset RENAME COLUMN ndarray_id TO storage_id;
ALTER TABLE storage_dataset RENAME COLUMN ndarray_version TO storage_version;
COMMENT ON TABLE storage_dataset IS 'Data: Association between storage and dataset instances (many-many)';

-- Name: ndarray_dimension; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
ALTER TABLE ndarray_dimension RENAME TO storage_dimension;
ALTER TABLE storage_dimension RENAME COLUMN ndarray_type_id TO storage_type_id;
ALTER TABLE storage_dimension RENAME COLUMN ndarray_id TO storage_id;
ALTER TABLE storage_dimension RENAME COLUMN ndarray_version TO storage_version;
ALTER TABLE storage_dimension RENAME COLUMN ndarray_dimension_index TO storage_dimension_index;
ALTER TABLE storage_dimension RENAME COLUMN ndarray_dimension_min TO storage_dimension_min;
ALTER TABLE storage_dimension RENAME COLUMN ndarray_dimension_max TO storage_dimension_max;
COMMENT ON TABLE storage_dimension IS 'Data: Association between storage and dimensions. Used to define attributes for each dimension in storage instances';

-- Name: ndarray_footprint; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
DROP TABLE ndarray_footprint_dimension cascade;
DROP TABLE ndarray_footprint cascade;

CREATE TABLE spatial_footprint
(
  spatial_footprint_id bigint NOT NULL,
  spatial_footprint_geometry geometry NOT NULL,
  CONSTRAINT pk_spatial_footprint PRIMARY KEY(spatial_footprint_id)
);

ALTER TABLE storage ADD COLUMN spatial_footprint_id bigint;

ALTER TABLE public.storage
  ADD CONSTRAINT fk_storage_spatial_footprint FOREIGN KEY (spatial_footprint_id)
      REFERENCES public.spatial_footprint (spatial_footprint_id) MATCH SIMPLE
      ON UPDATE CASCADE ON DELETE CASCADE;


CREATE INDEX fki_storage_spatial_footprint
  ON public.storage
  USING btree
  (spatial_footprint_id);


-- Name: ndarray_type_measurement_type; Type: TABLE; Schema: public; Owner: cube_admin; Tablespace: 
ALTER TABLE ndarray_type_measurement_type RENAME TO storage_type_measurement_type;
ALTER TABLE storage_type_measurement_type RENAME COLUMN ndarray_type_id TO storage_type_id;
COMMENT ON TABLE storage_type_measurement_type IS 'Configuration: Associations between n-dimensional data structure types and measurement types (i.e. variables) (many-many)';

-- Name: ndarray_id_seq; Type: SEQUENCE; Schema: public; Owner: cube_admin
ALTER SEQUENCE ndarray_id_seq RENAME TO storage_id_seq;




-- Name: pk_ndarray; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
ALTER TABLE ONLY storage
    RENAME CONSTRAINT pk_ndarray TO pk_storage;
-- Name: pk_ndarray_dataset; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
ALTER TABLE ONLY storage_dataset
    RENAME CONSTRAINT pk_ndarray_dataset TO pk_storage_dataset;
-- Name: pk_ndarray_dimension; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
ALTER TABLE ONLY storage_dimension
    RENAME CONSTRAINT pk_ndarray_dimension TO pk_storage_dimension;
-- Name: pk_ndarray_type; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
ALTER TABLE ONLY storage_type
    RENAME CONSTRAINT pk_ndarray_type TO pk_storage_type;
-- Name: pk_ndarray_type_dimension; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
ALTER TABLE ONLY storage_type_dimension
    RENAME CONSTRAINT pk_ndarray_type_dimension TO pk_storage_type_dimension;
-- Name: pk_ndarray_type_dimension_property; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
ALTER TABLE ONLY storage_type_dimension_property
    RENAME CONSTRAINT pk_ndarray_type_dimension_property TO pk_storage_type_dimension_property;
-- Name: pk_ndarray_type_measurement_type; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
ALTER TABLE ONLY storage_type_measurement_type
    RENAME CONSTRAINT pk_ndarray_type_measurement_type TO pk_storage_measurement_type;

-- Name: uq_dataset_type_measurement_type_ndarray_type_id_measurement_ty; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
ALTER TABLE ONLY dataset_type_measurement_type
    RENAME CONSTRAINT uq_dataset_type_measurement_type_ndarray_type_id_measurement_ty TO uq_dataset_type_measurement_type_dataset_type;
-- Name: uq_ndarray_ndarray_location; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
ALTER TABLE ONLY storage
    RENAME CONSTRAINT uq_ndarray_ndarray_location TO uq_storage_storage_location;
-- Name: uq_ndarray_type_dimension_ndarray_type_dimension; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
ALTER TABLE ONLY storage_type_dimension
    RENAME CONSTRAINT uq_ndarray_type_dimension_ndarray_type_dimension TO uq_storage_type_dimension_storage_type_dimension; 
-- Name: CONSTRAINT uq_ndarray_type_dimension_ndarray_type_dimension ON ndarray_type_dimension; Type: COMMENT; Schema: public; Owner: cube_admin
COMMENT ON CONSTRAINT uq_storage_type_dimension_storage_type_dimension ON storage_type_dimension IS 'Unique constraint to ensure each dimension is only represented once in each storage_type';
-- Name: uq_ndarray_type_measurement_type_ndarray_type_id_measurement_ty; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
ALTER TABLE ONLY storage_type_measurement_type
    RENAME CONSTRAINT uq_ndarray_type_measurement_type_ndarray_type_id_measurement_ty TO uq_storage_type_measurement_type_storage_type_id_measurement_ty;
-- Name: uq_ndarray_type_ndarray_type_name; Type: CONSTRAINT; Schema: public; Owner: cube_admin; Tablespace: 
ALTER TABLE ONLY storage_type
    RENAME CONSTRAINT uq_ndarray_type_ndarray_type_name TO uq_storage_type_storage_type_name;

-- Name: fk_ndarray_dataset_dataset; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
ALTER TABLE ONLY storage_dataset
    RENAME CONSTRAINT fk_ndarray_dataset_dataset TO fk_storage_dataset_dataset;
-- Name: fk_ndarray_dataset_ndarray; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
ALTER TABLE ONLY storage_dataset
    RENAME CONSTRAINT fk_ndarray_dataset_ndarray TO fk_storage_dataset_storage;
-- Name: fk_ndarray_dimension_ndarray; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
ALTER TABLE ONLY storage_dimension
    RENAME CONSTRAINT fk_ndarray_dimension_ndarray TO fk_storage_dimension_storage ;
-- Name: fk_ndarray_dimension_ndarray_type_dimension; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
ALTER TABLE ONLY storage_dimension
    RENAME CONSTRAINT fk_ndarray_dimension_ndarray_type_dimension TO fk_storage_dimension_storage_type_dimension;
-- Name: fk_ndarray_ndarray_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
ALTER TABLE ONLY storage
    RENAME CONSTRAINT fk_ndarray_ndarray_type TO fk_storage_storage_type;
-- Name: fk_ndarray_type_dimension_attribute_ndarray_type_dimension; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
ALTER TABLE ONLY storage_type_dimension_property
    RENAME CONSTRAINT fk_ndarray_type_dimension_attribute_ndarray_type_dimension TO fk_storage_type_dimension_attribute_storage_type_dimension;
-- Name: fk_ndarray_type_dimension_dimension_domain; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
ALTER TABLE ONLY storage_type_dimension
    RENAME CONSTRAINT fk_ndarray_type_dimension_dimension_domain TO fk_storage_type_dimension_dimension_domain;
-- Name: fk_ndarray_type_dimension_indexing_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
ALTER TABLE ONLY storage_type_dimension
    RENAME CONSTRAINT fk_ndarray_type_dimension_indexing_type TO fk_storage_type_dimension_indexing_type;
-- Name: fk_ndarray_type_dimension_ndarray_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
ALTER TABLE ONLY storage_type_dimension
    RENAME CONSTRAINT fk_ndarray_type_dimension_ndarray_type TO fk_storage_type_dimension_storage_type;
-- Name: fk_ndarray_type_dimension_property_property; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
ALTER TABLE ONLY storage_type_dimension_property
    RENAME CONSTRAINT fk_ndarray_type_dimension_property_property TO fk_storage_type_dimension_property_property;
-- Name: fk_ndarray_type_dimension_reference_system; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
ALTER TABLE ONLY storage_type_dimension
    RENAME CONSTRAINT fk_ndarray_type_dimension_reference_system TO fk_storage_type_dimension_reference_system;
-- Name: fk_ndarray_type_measurement_type_datatype; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
ALTER TABLE ONLY storage_type_measurement_type
    RENAME CONSTRAINT fk_ndarray_type_measurement_type_datatype TO fk_storage_type_measurement_type_datatype;
-- Name: fk_ndarray_type_measurement_type_measurement_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
ALTER TABLE ONLY storage_type_measurement_type
    RENAME CONSTRAINT fk_ndarray_type_measurement_type_measurement_type TO fk_storage_type_measurement_type_measurement_type;
-- Name: fk_ndarray_type_measurement_type_ndarray_type; Type: FK CONSTRAINT; Schema: public; Owner: cube_admin
ALTER TABLE ONLY storage_type_measurement_type
    RENAME CONSTRAINT fk_ndarray_type_measurement_type_ndarray_type TO fk_storage_type_measurement_type_storage_type;

-- Name: fki_ndarray_dataset_dataset; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
ALTER INDEX fki_ndarray_dataset_dataset RENAME TO fki_storage_dataset_dataset;
-- Name: fki_ndarray_dataset_ndarray; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
ALTER INDEX fki_ndarray_dataset_ndarray RENAME TO fki_storage_dataset_storage;
-- Name: fki_ndarray_dimension_ndarray; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
ALTER INDEX fki_ndarray_dimension_ndarray RENAME TO fki_storage_dimension_storage;
-- Name: fki_ndarray_dimension_ndarray_type_dimension; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
ALTER INDEX fki_ndarray_dimension_ndarray_type_dimension RENAME TO fki_storage_dimension_storage_type_dimension;
-- Name: fki_ndarray_ndarray_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
ALTER INDEX fki_ndarray_ndarray_type RENAME TO fki_storage_storage_type;
-- Name: fki_ndarray_type_dimension_attribute_ndarray_type_dimension; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
ALTER INDEX fki_ndarray_type_dimension_attribute_ndarray_type_dimension RENAME TO fki_storage_type_dimension_attribute_storage_type_dimension;
-- Name: fki_ndarray_type_dimension_attribute_property; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
ALTER INDEX fki_ndarray_type_dimension_attribute_property RENAME TO fki_storage_type_dimension_attribute_property;
-- Name: fki_ndarray_type_dimension_dimension_domain; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
ALTER INDEX fki_ndarray_type_dimension_dimension_domain RENAME TO fki_storage_type_dimension_dimension_domain;
-- Name: fki_ndarray_type_dimension_indexing_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
ALTER INDEX fki_ndarray_type_dimension_indexing_type RENAME TO fki_storage_type_dimension_indexing_type;
-- Name: fki_ndarray_type_dimension_ndarray_type_id_fkey; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
ALTER INDEX fki_ndarray_type_dimension_ndarray_type_id_fkey RENAME TO fki_storage_type_dimension_storage_type_id_fkey;
-- Name: fki_ndarray_type_masurement_type_datatype; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
ALTER INDEX fki_ndarray_type_masurement_type_datatype RENAME TO fki_storage_type_masurement_type_datatype;
-- Name: fki_ndarray_type_measurement_type_measurement_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
ALTER INDEX fki_ndarray_type_measurement_type_measurement_type RENAME TO fki_storage_type_measurement_type_measurement_type;
-- Name: fki_ndarray_type_measurement_type_ndarray_type; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
ALTER INDEX fki_ndarray_type_measurement_type_ndarray_type RENAME TO fki_storage_type_measurement_type_storage_type;
-- Name: idx_ndarray_dimension_ndarray_dimension_index; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
ALTER INDEX idx_ndarray_dimension_ndarray_dimension_index RENAME TO idx_storage_dimension_storage_dimension_index;
-- Name: idx_ndarray_dimension_ndarray_dimension_max; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
ALTER INDEX idx_ndarray_dimension_ndarray_dimension_max RENAME TO idx_storage_dimension_storage_dimension_max;
-- Name: idx_ndarray_dimension_ndarray_dimension_min; Type: INDEX; Schema: public; Owner: cube_admin; Tablespace: 
ALTER INDEX idx_ndarray_dimension_ndarray_dimension_min RENAME TO idx_storage_dimension_storage_dimension_min;


-- Materialized View: public.ndarray_type_dimension_view
DROP MATERIALIZED VIEW public.ndarray_type_dimension_view cascade;
CREATE MATERIALIZED VIEW public.storage_type_dimension_view AS 
 SELECT storage_type.storage_type_id,
    storage_type_dimension.dimension_order AS creation_order,
    domain.domain_id,
    dimension.dimension_id,
    storage_type_dimension.reference_system_id,
    storage_type.storage_type_name AS storage_type_name,
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
   FROM storage_type storage_type(storage_type_id, storage_type_name, storage_type_tag)
     JOIN storage_type_dimension storage_type_dimension(storage_type_id, domain_id, dimension_id, dimension_order, dimension_extent, dimension_elements, dimension_cache, dimension_origin, indexing_type_id, reference_system_id, index_reference_system_id) USING (storage_type_id)
     JOIN dimension_domain USING (domain_id, dimension_id)
     JOIN domain USING (domain_id)
     JOIN dimension USING (dimension_id)
     JOIN reference_system USING (reference_system_id)
     JOIN reference_system index_reference_system ON storage_type_dimension.index_reference_system_id = index_reference_system.reference_system_id
     JOIN indexing_type USING (indexing_type_id)
  ORDER BY storage_type.storage_type_id, storage_type_dimension.dimension_order
WITH DATA;

ALTER TABLE public.storage_type_dimension_view
  OWNER TO cube_admin;
GRANT ALL ON TABLE public.storage_type_dimension_view TO cube_admin;
GRANT ALL ON TABLE public.storage_type_dimension_view TO cube_admin_group;
GRANT SELECT ON TABLE public.storage_type_dimension_view TO cube_user_group;

REFRESH MATERIALIZED VIEW public.storage_type_dimension_view;

-- Materialized View: public.dimension_indices_view
-- DROP MATERIALIZED VIEW public.dimension_indices_view;
CREATE MATERIALIZED VIEW public.dimension_indices_view AS 
 SELECT storage_type_dimension_view.storage_type_id,
    storage_type_dimension_view.domain_id,
    storage_type_dimension_view.dimension_id,
    reference_system_indexing.reference_system_id,
    reference_system_indexing.array_index,
    reference_system_indexing.indexing_name,
    reference_system_indexing.measurement_metatype_id,
    reference_system_indexing.measurement_type_id
   FROM storage_type_dimension_view
     JOIN reference_system_indexing USING (reference_system_id)
  ORDER BY storage_type_dimension_view.storage_type_id, storage_type_dimension_view.dimension_id, reference_system_indexing.array_index
WITH NO DATA;

ALTER TABLE public.dimension_indices_view
  OWNER TO cube_admin;
GRANT ALL ON TABLE public.dimension_indices_view TO cube_admin;
GRANT ALL ON TABLE public.dimension_indices_view TO cube_admin_group;
GRANT SELECT ON TABLE public.dimension_indices_view TO cube_user_group;

REFRESH MATERIALIZED VIEW public.dimension_indices_view;

-- Materialized View: public.dimension_properties_view
-- DROP MATERIALIZED VIEW public.dimension_properties_view;
CREATE MATERIALIZED VIEW public.dimension_properties_view AS 
 SELECT storage_type_dimension_view.storage_type_id,
    storage_type_dimension_view.domain_id,
    storage_type_dimension_view.dimension_id,
    storage_type_dimension_view.dimension_name,
    property.property_name,
    storage_type_dimension_property.attribute_string,
    datatype.datatype_name
   FROM storage_type_dimension_view
     JOIN storage_type_dimension_property storage_type_dimension_property(storage_type_id, domain_id, dimension_id, property_id, attribute_string) USING (storage_type_id, domain_id, dimension_id)
     JOIN property USING (property_id)
     JOIN datatype USING (datatype_id)
  ORDER BY storage_type_dimension_view.storage_type_id, storage_type_dimension_view.creation_order, property.property_name
WITH NO DATA;

ALTER TABLE public.dimension_properties_view
  OWNER TO cube_admin;
GRANT ALL ON TABLE public.dimension_properties_view TO cube_admin;
GRANT ALL ON TABLE public.dimension_properties_view TO cube_admin_group;
GRANT SELECT ON TABLE public.dimension_properties_view TO cube_user_group;

REFRESH MATERIALIZED VIEW public.dimension_properties_view;

GRANT ALL ON ALL TABLES IN SCHEMA public TO cube_admin;
GRANT ALL ON ALL TABLES IN SCHEMA public TO cube_admin_group;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO cube_user_group;

GRANT ALL ON ALL TABLES IN SCHEMA earth_observation TO cube_admin;
GRANT ALL ON ALL TABLES IN SCHEMA earth_observation TO cube_admin_group;
GRANT SELECT ON ALL TABLES IN SCHEMA earth_observation TO cube_user_group;

GRANT ALL ON ALL TABLES IN SCHEMA ztmp TO cube_admin;
GRANT ALL ON ALL TABLES IN SCHEMA ztmp TO cube_admin_group;
GRANT ALL ON ALL TABLES IN SCHEMA ztmp TO cube_user_group;
