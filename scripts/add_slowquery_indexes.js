// 慢查询优化索引补齐脚本 (2026-07-02)
// 依据 慢查询日志_2026-07-02 分析：这些集合线上缺失关键索引(建索引代码在被注释的 createDatabaseIndex 里未执行)。
// 用法：mongosh -u root -p openIM123 --authenticationDatabase admin openim_v3 scripts/add_slowquery_indexes.js
// 说明：background:true 兼容旧版 Mongo(4.2+ 会忽略，采用不阻塞的混合构建)；createIndex 幂等，已存在则跳过。

const tasks = [
  // attribute 被 transaction_record / organization_user 实名统计 / app_log / operation_log 四类按 user_id lookup
  ["attribute",          { user_id: 1 }],
  ["attribute",          { is_real_name_verified: 1, user_id: 1 }],
  // transaction_record 列表/统计 $match {org_id, transaction_type:{$nin:[4,7]}}
  ["transaction_record", { org_id: 1, transaction_type: 1 }],
  // operation_log 分页 $match{org_id} + $sort{operation_time:-1}
  ["operation_log",      { org_id: 1, operation_time: -1 }],
  // checkin(91万) 统计/count 按 date 范围 + org_id；含 im_server_user_id 便于去重覆盖
  ["checkin",            { org_id: 1, date: 1, im_server_user_id: 1 }],
  // organization_user 实名统计/成员列表 $match {organization_id, role:{$in:[...]}}
  ["organization_user",  { organization_id: 1, role: 1 }],
];

tasks.forEach(function (t) {
  const coll = t[0], key = t[1];
  const t0 = Date.now();
  try {
    const name = db.getCollection(coll).createIndex(key, { background: true });
    print("OK   " + coll + " " + JSON.stringify(key) + " -> " + name + "  (" + (Date.now() - t0) + "ms)");
  } catch (e) {
    print("FAIL " + coll + " " + JSON.stringify(key) + " -> " + e.message);
  }
});
print("done.");
