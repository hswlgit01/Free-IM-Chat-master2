package cache

//
//const (
//	CacheExpireTime = time.Second * 60
//)
//
//type OrgUserCacheRedis struct {
//	rdb        redis.UniversalClient
//	expireTime time.Duration
//	rcClient   *cacheRedis.RocksCacheClient
//	Dao        *model.OrganizationDao
//}
//
//func NewOrgUserCacheRedis(rdb redis.UniversalClient, db *mongo.Database) *OrgCacheRedis {
//	rc := cacheRedis.NewRocksCacheClient(rdb)
//	return &OrgCacheRedis{
//		rdb:        rdb,
//		expireTime: CacheExpireTime,
//		rcClient:   rc,
//		Dao:        model.NewOrganizationDao(db),
//	}
//}
//
//func (u *OrgUserCacheRedis) GetById(ctx context.Context, id primitive.ObjectID) (*model.Organization, error) {
//	return cacheRedis.GetCache(ctx, u.rcClient, fmt.Sprintf("C_ORG_ID:%s", id.Hex()), u.expireTime, func(ctx context.Context) (*model.Organization, error) {
//		return u.Dao.GetById(ctx, id)
//	})
//}
//
//func (u *OrgUserCacheRedis) GetByIdAndStatus(ctx context.Context, id primitive.ObjectID, status model.OrganizationStatus) (*model.Organization, error) {
//	return cacheRedis.GetCache(ctx, u.rcClient, fmt.Sprintf("C_ORG_ID_%s:%s", status, id.Hex()), u.expireTime, func(ctx context.Context) (*model.Organization, error) {
//		return u.Dao.GetByIdAndStatus(ctx, id, status)
//	})
//}
