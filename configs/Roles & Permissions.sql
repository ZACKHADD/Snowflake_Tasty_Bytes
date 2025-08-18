
/**************************************************************************
                    Snowflake standard roles
 **************************************************************************

ROLES IN SNOWFLAKE
└── ACCOUNTADMIN (Top-level superuser)
    ├─ Purpose:
    │    Full control over everything in the account
    ├─ Privileges & Actions:
    │    • Manage users, roles, resource monitors, integrations
    │    • Create/grant any role or privilege
    │    • Override all access controls
    └─ Typical Users:
         CDO, Cloud Admins, Lead Architects

    └── SECURITYADMIN (Access control manager)
        ├─ Purpose:
        │    Manage users, roles, and grants
        ├─ Privileges & Actions:
        │    • Create and manage users, roles, passwords
        │    • Grant/revoke roles and object privileges
        │    • Does not automatically have access to data
        └─ Typical Users:
             Security Officers, IAM Engineers

        └── USERADMIN (User and role creator)
            ├─ Purpose:
            │    Create users and roles
            ├─ Privileges & Actions:
            │    • Create users and roles
            │    • Cannot assign account-level roles (e.g., ACCOUNTADMIN)
            └─ Typical Users:
                 HR IT Admins, Org Admins

    └── SYSADMIN (Resource/object manager)
        ├─ Purpose:
        │    Manage databases, schemas, tables, warehouses
        ├─ Privileges & Actions:
        │    • Create and manage objects (DBs, tables, etc.)
        │    • Grant object access to other roles
        │    • Cannot manage users/roles unless granted
        └─ Typical Users:
             Data Engineers, Infrastructure Leads

        └── PUBLIC (Minimal access role)
            ├─ Purpose:
            │    Default role assigned to every user
            ├─ Privileges & Actions:
            │    • Usually has no sensitive privileges
            │    • Can be granted harmless defaults (e.g., read-only views)
            └─ Typical Users:
                 All users (default fallback role)

ORGADMIN (Organization-level admin)
└─ Purpose:
     Manage multiple Snowflake accounts in an org (Business Critical+ only)
└─ Privileges & Actions:
     • Create/manage accounts
     • Set org-wide policies
└─ Typical Users:
     Enterprise Org Admins

**************************************************************************/


/**************************************************************************
                    Typical roles architecture in real world
 **************************************************************************
                          SECURITYADMIN
                                │
                             SYSADMIN
                                │
    ┌────────────────┬──────────────┴─────────────┬────────────────────┐
    ▼              ▼                         ▼                  ▼
FINANCE_ADMIN   HR_ADMIN                 SALES_ADMIN        SHARED_ANALYST
    │              │                         │                  │
    │              │                         │                  ▼
    │              │                         │            Read-only access
    ▼              ▼                         ▼          to cross-domain views
FINANCE_ENGINEER  HR_ENGINEER          SALES_ENGINEER
    │              │                         │
    ▼              ▼                         ▼
FINANCE_ANALYST  HR_ANALYST           SALES_ANALYST

 **************************************************************************/

USE ROLE SECURITYADMIN;

CREATE ROLE IF NOT EXISTS TASTY_ADMIN
    COMMENT = 'role of admin of tasty scope';
CREATE ROLE IF NOT EXISTS DATA_ENGINEER
    COMMENT = 'role of data engineer of tasty scope';
CREATE ROLE IF NOT EXISTS Data_Analyst
    COMMENT = 'role of data analyst of tasty scope';

-- Create the roles hierarchy

GRANT ROLE TASTY_ADMIN TO ROLE SYSADMIN;

GRANT ROLE DATA_ENGINEER TO ROLE TASTY_ADMIN;   

GRANT ROLE Data_Analyst TO ROLE TASTY_ADMIN;  


-- Note that when we create a role in snowflake, by defautl it cannot do anything unless we explicitly grant it with preveliges !!  


/**************************************************************************
                    Grants and previleges
 **************************************************************************
 
Grants follow a hierarchy up-down : To get a previlege on an item we need previlege on the ones above it !

Grants in Snowflake
└── Account-Level
    ├── CREATE DATABASE              (create new databases)
    ├── CREATE WAREHOUSE             (create warehouses)
    ├── CREATE USER                  (create users)
    ├── CREATE ROLE                  (create roles)
    ├── CREATE INTEGRATION           (create external integrations)
    ├── CREATE SHARE                 (create data shares)
    ├── CREATE NETWORK POLICY        (create network policies)
    ├── CREATE DATA EXCHANGE LISTING (create marketplace listings)
    ├── CREATE REPLICATION GROUP     (create replication groups)
    ├── CREATE FAILOVER GROUP        (create failover groups)
    ├── APPLY MASKING POLICY         (apply column-level masking)
    ├── APPLY ROW ACCESS POLICY      (apply RLS policies)
    ├── APPLY TAG                    (assign tags to objects)
    ├── MONITOR USAGE                (access billing/data usage)
    ├── EXECUTE TASK                 (run tasks across account)
    ├── IMPORTED PRIVILEGES          (use shared data)
    ├── BIND SERVICE PRINCIPAL       (for Snowpark Container Services)
    ├── OWNERSHIP                    (full control)

└── Database-Level
    ├── USAGE                        (reference database in queries)
    ├── CREATE SCHEMA                (create schemas in database)
    ├── MONITOR                      (view queries/metadata)
    ├── MODIFY                       (rename/alter the database)
    ├── IMPORTED PRIVILEGES          (access external shared data)
    ├── OWNERSHIP                    (full control)

└── Schema-Level
    ├── USAGE                        (reference schema)
    ├── CREATE TABLE
    ├── CREATE VIEW
    ├── CREATE MATERIALIZED VIEW
    ├── CREATE STAGE
    ├── CREATE FILE FORMAT
    ├── CREATE FUNCTION              (scalar/table UDFs)
    ├── CREATE PROCEDURE
    ├── CREATE SEQUENCE
    ├── CREATE PIPE
    ├── CREATE STREAM
    ├── CREATE TASK
    ├── CREATE MASKING POLICY
    ├── CREATE ROW ACCESS POLICY
    ├── CREATE TAG
    ├── MONITOR
    ├── MODIFY
    ├── OWNERSHIP

└── Object-Level

    ├── Table / View / Materialized View
    │   ├── SELECT
    │   ├── INSERT
    │   ├── UPDATE
    │   ├── DELETE
    │   ├── TRUNCATE
    │   ├── REFERENCES                (for foreign keys)
    │   ├── OWNERSHIP

    ├── External Table
    │   ├── SELECT
    │   ├── REFERENCES
    │   ├── OWNERSHIP

    ├── Stage
    │   ├── USAGE
    │   ├── READ
    │   ├── WRITE
    │   ├── OWNERSHIP

    ├── Sequence
    │   ├── USAGE
    │   ├── OWNERSHIP

    ├── File Format
    │   ├── USAGE
    │   ├── OWNERSHIP

    ├── Stream
    │   ├── SELECT
    │   ├── OWNERSHIP

    ├── Pipe
    │   ├── MONITOR
    │   ├── OPERATE
    │   ├── OWNERSHIP

    ├── Task
    │   ├── OPERATE
    │   ├── OWNERSHIP

    ├── Function / Procedure
    │   ├── USAGE
    │   ├── CALLER
    │   ├── OWNERSHIP
    │   └── Note: EXECUTE context can be:
    │          - EXECUTE AS CALLER (requires CALLER privilege)
    │          - EXECUTE AS OWNER (requires no extra privilege)

    ├── Masking Policy
    │   ├── APPLY
    │   ├── OWNERSHIP

    ├── Row Access Policy
    │   ├── APPLY
    │   ├── OWNERSHIP

    ├── Tag
    │   ├── APPLY
    │   ├── OWNERSHIP

└── Warehouse-Level
    ├── USAGE
    ├── OPERATE
    ├── MODIFY
    ├── MONITOR
    ├── OWNERSHIP

└── Integration-Level
    ├── USAGE
    ├── OWNERSHIP

└── Future Grants
    └── GRANT <privilege> ON FUTURE <object_type>
        IN [DATABASE | SCHEMA] <name> TO ROLE <role>
        (e.g., SELECT on future tables)

└── Special Options
    ├── WITH GRANT OPTION            (allows re-granting the privilege)
    └── OWNERSHIP                    (implies full control + grant option)


 **************************************************************************/
--!!!! Note : To revoke grants we simply replace GRANT with REVOKE and TO with FROM 

-- REVOKE SELECT ON TABLE finance_db.modelled.transactions FROM ROLE finance_da;  

-- We will give the tasty admin the ability to create warehouses, databases and every object needed
USE ROLE ACCOUNTADMIN;

GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE TASTY_ADMIN;

------------

USE ROLE SECURITYADMIN;

GRANT OWNERSHIP ON DATABASE TASTY_BYTES TO ROLE TASTY_ADMIN;

SHOW GRANTS TO ROLE TASTY_ADMIN;

-------------
-- The data engineer role must have access to the snowflake database to monitor query history, jobs , users and so on !
-- The tasty_admin will inherit that since data engineer role is attached to the tasty admin !

GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE DATA_ENGINEER; -- Since Snowflake is external database shared by snowflake !


-- Let's grant the DATA_ENGINEER preveliges to use the database and its objects !
GRANT USAGE ON DATABASE TASTY_BYTES TO ROLE DATA_ENGINEER;
GRANT ALL ON ALL SCHEMAS IN DATABASE TASTY_BYTES TO ROLE DATA_ENGINEER;

-- Future Grants to role DATA_ENGINEER !
GRANT ALL ON FUTURE SCHEMAS IN DATABASE TASTY_BYTES TO ROLE DATA_ENGINEER;

-- Since all the objects will be created by the data engineer the only objects the admin need to grant previleges on to data engineer 
-- are masking polices, tags and row policies, file formats .. !
/* Example would be : 
        GRANT APPLY ON MASKING POLICY my_db.my_schema.mask_email TO ROLE data_engineer"
 */

GRANT USAGE ON STAGE S3_TASTY_FILES TO ROLE DATA_ENGINEER;

GRANT USAGE ON FILE FORMAT S3_FF_TASTY TO ROLE DATA_ENGINEER;

-- Let's grant the DATA_ENGINEER preveliges to use the warehouse !

GRANT ALL ON WAREHOUSE DE_WH TO ROLE DATA_ENGINEER;


-- Now the data engineer can start creating tables and populate them !

-- To check the previleges, Run the two following queries as single block !
SHOW GRANTS TO ROLE DATA_ENGINEER;
SELECT *
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
WHERE "grantee_name" = 'DATA_ENGINEER' AND "granted_on" = 'F';