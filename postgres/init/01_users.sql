
-- Set variables từ .env để dùng trong SQL script này
\set replication_user `echo "$REPLICATION_USER"`
\set replication_password `echo "$REPLICATION_PASSWORD"`
\set app_user `echo "$APP_USER"`
\set app_password `echo "$APP_PASSWORD"`

-- Replication user: Debezium sẽ dùng để đọc WAL
-- Attribute REPLICATION = cho phép gọi logical replication API
-- KHÔNG cấp INSERT/UPDATE/DELETE → khớp constraint "source read-only"
-- LOGIN để có thể login vào posgret chưa vào database cụ thể nào, còn CONNECT sẽ cấp quyền vào database cụ thể (bankdb)
CREATE ROLE :replication_user
    WITH REPLICATION
         LOGIN
         PASSWORD :'replication_password';

-- Cần CONNECT để login vào database
GRANT CONNECT ON DATABASE bankdb TO :replication_user;

-- USAGE trên schema để "nhìn thấy" được các object trong schema public
GRANT USAGE ON SCHEMA public TO :replication_user;

-- SELECT trên tất cả bảng sẽ tạo trong tương lai (initial snapshot của Debezium)
-- Đây là Postgres syntax đặc biệt - apply cho object SẼ TẠO sau này
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT ON TABLES TO :replication_user;

-- App user: data generator dùng để INSERT/UPDATE giả app thật

-- KHÔNG có REPLICATION - app thật không cần đọc WAL
CREATE ROLE :app_user
    WITH LOGIN
         PASSWORD :'app_password';

GRANT CONNECT ON DATABASE bankdb TO :app_user;
GRANT USAGE, CREATE ON SCHEMA public TO :app_user;

-- App user cần full DML để giả lập app banking
-- mọi table/sequence tạo trong tương lai  app_user tự động có quyền sử dụng
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO :app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT USAGE, SELECT ON SEQUENCES TO :app_user;

-- Verify: in ra để check khi chạy lần đầu
SELECT
    rolname,
    rolcanlogin AS can_login,
    rolreplication AS can_replicate,
    rolsuper AS is_super
FROM pg_roles
WHERE rolname IN (:'replication_user', :'app_user')
ORDER BY rolname;
