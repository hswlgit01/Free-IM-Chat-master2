// dawn 2026-05-05 修复聊天记录审计查询：为 operation_log 增加后台消息操作检索索引。
const dbName =
  typeof process !== 'undefined' && process.env && process.env.MONGO_DB
    ? process.env.MONGO_DB
    : 'openim_v3';

db = db.getSiblingDB(dbName);

db.operation_log.createIndex(
  { org_id: 1, operation_type: 1, operation_time: -1 },
  { name: 'idx_org_operation_type_time' },
);

db.operation_log.createIndex(
  { org_id: 1, 'details.target_user_id': 1, operation_time: -1 },
  {
    name: 'idx_org_chat_target_user_time',
    partialFilterExpression: { 'details.target_user_id': { $exists: true } },
  },
);

db.operation_log.createIndex(
  { org_id: 1, 'details.conversation_id': 1, operation_time: -1 },
  {
    name: 'idx_org_chat_conversation_time',
    partialFilterExpression: { 'details.conversation_id': { $exists: true } },
  },
);

db.operation_log.createIndex(
  { org_id: 1, 'details.server_msg_id': 1 },
  {
    name: 'idx_org_chat_server_msg_id',
    partialFilterExpression: { 'details.server_msg_id': { $exists: true } },
  },
);

db.operation_log.createIndex(
  { org_id: 1, 'details.client_msg_id': 1 },
  {
    name: 'idx_org_chat_client_msg_id',
    partialFilterExpression: { 'details.client_msg_id': { $exists: true } },
  },
);

print(`operation_log indexes ensured on database: ${dbName}`);
