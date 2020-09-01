package main

import (
	m2s "github.com/luispater/mysql2elasticsearch"
	"time"
)

type JdProduct struct {
	ID                 uint       `gorm:"primary_key" json:"id"`
	SkuId              int64      `gorm:"column:sku_id" json:"sku_id"`
	SkuName            string     `gorm:"column:sku_name" json:"sku_name"`
	SpuId              int64      `gorm:"column:spu_id" json:"spu_id"`
	Price              float64    `gorm:"column:price" json:"price"`
	EliteId            int64      `gorm:"column:elite_id" json:"elite_id"`
	EliteName          string     `gorm:"column:elite_name" json:"elite_name"`
	ShopId             int64      `gorm:"column:shop_id" json:"shop_id"`
	ShopName           string     `gorm:"column:shop_name" json:"shop_name"`
	BrandCode          string     `gorm:"column:brand_code" json:"brand_code"`
	BrandName          string     `gorm:"column:brand_name" json:"brand_name"`
	Category1Id        int64      `gorm:"column:category_1_id" json:"category_1_id"`
	Category1Name      string     `gorm:"column:category_1_name" json:"category_1_name"`
	Category2Id        int64      `gorm:"column:category_2_id" json:"category_2_id"`
	Category2Name      string     `gorm:"column:category_2_name" json:"category_2_name"`
	Category3Id        int64      `gorm:"column:category_3_id" json:"category_3_id"`
	Category3Name      string     `gorm:"column:category_3_name" json:"category_3_name"`
	Comments           int64      `gorm:"column:comments" json:"comments"`
	GoodCommentsShare  float64    `gorm:"column:good_comments_share" json:"good_comments_share"`
	Commission         float64    `gorm:"column:commission" json:"commission"`
	CommissionShare    float64    `gorm:"column:commission_share" json:"commission_share"`
	CouponCommission   float64    `gorm:"column:coupon_commission" json:"coupon_commission"`
	InOrderComm30Days  float64    `gorm:"column:in_order_comm_30_days" json:"in_order_comm_30_days"`
	InOrderCount30Days int64      `gorm:"column:in_order_count_30_days" json:"in_order_count_30_days"`
	IsHot              bool       `gorm:"column:is_hot" json:"is_hot"`
	IsJdSale           bool       `gorm:"column:is_jd_sale" json:"is_jd_sale"`
	HasCoupon          bool       `gorm:"column:has_coupon" json:"has_coupon"`
	MaterialUrl        string     `gorm:"column:material_url" json:"material_url"`
	Owner              string     `gorm:"column:owner" json:"owner"`
	PingouStartTime    *time.Time `gorm:"column:pingou_start_time" json:"pingou_start_time"`
	PingouEndTime      *time.Time `gorm:"column:pingou_end_time" json:"pingou_end_time"`
	PingouPrice        float64    `gorm:"column:pingou_price" json:"pingou_price"`
	PingouTmCount      int64      `gorm:"column:pingou_tm_count" json:"pingou_tm_count"`
	PingouUrl          string     `gorm:"column:pingou_url" json:"pingou_url"`
	SeckillStartTime   *time.Time `gorm:"column:seckill_start_time" json:"seckill_start_time"`
	SeckillEndTime     *time.Time `gorm:"column:seckill_end_time" json:"seckill_end_time"`
	SeckillOriPrice    float64    `gorm:"column:seckill_ori_price" json:"seckill_ori_price"`
	SeckillPrice       float64    `gorm:"column:seckill_price" json:"seckill_price"`
	Description        string     `gorm:"column:description" json:"description"`
	ImageUrls          string     `gorm:"column:image_urls" json:"image_urls"`
	Status             bool       `gorm:"column:status" json:"status"`
	CreatedAt          time.Time  `json:"createTime"`
	UpdatedAt          time.Time  `json:"updateTime"`
	DeletedAt          *time.Time `sql:"index" json:"-"`
}

func main() {
	sync, err := m2s.NewSyncMySQLToElasticSearch("192.168.1.110:3306", "root", "", "coupon_leader", "http://192.168.2.10:9200/", "", "", m2s.SyncMySQLToElasticSearchOption{
		IndexNameSeparator: "-",
		QueueSize:          10000,
		MaxQueueSize:       30000,
	})
	if err != nil {
		panic(err)
	}
	err = sync.RegisterTable("jd_products", JdProduct{})
	if err != nil {
		panic(err)
	}
	// err = sync.Sync()
	err = sync.SyncFrom("mysql-bin.000019", 109115627)
	if err != nil {
		panic(err)
	}

}
