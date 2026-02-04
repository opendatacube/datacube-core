=============================================
Database Schema and User Management Utilities
=============================================

(For downstream apps and services with their own database schemas)
------------------------------------------------------------------

.. currentmodule:: datacube.drivers.common_psql

.. autosummary::
   :toctree: generate/

   UserRoleBase
   create_user
   drop_users
   ensure_role
   grant_role
   has_role
   has_role_membership
   has_roles
   create_schema
   drop_schema
   has_schema
   transfers_required
   transfer_ownership
   as_role
   ensure_extension
   escape_pg_identifier
   get_connection_info
