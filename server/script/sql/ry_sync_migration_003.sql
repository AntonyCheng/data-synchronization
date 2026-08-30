-- Metadata exploration and MySQL CDC prerequisite permissions.
-- Run against ry-vue. This migration is safe to re-run.

insert ignore into sys_menu values
(1761400000000002040, '数据源元数据探查', 1761400000000002001, 6, 'data-source', '', '', 'N', 'Y', 'F', '0', '0', 'sync:data-source:metadata', '#', '', '', 1761000000000000103, 1761100000000000001, sysdate(), null, null, ''),
(1761400000000002041, 'CDC 前置检查', 1761400000000002001, 7, 'data-source', '', '', 'N', 'Y', 'F', '0', '0', 'sync:data-source:cdc-precheck', '#', '', '', 1761000000000000103, 1761100000000000001, sysdate(), null, null, '');

insert ignore into sys_role_menu (role_id, menu_id)
select 1761300000000000003, menu_id from sys_menu where menu_id in (1761400000000002040, 1761400000000002041);
