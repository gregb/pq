// Package oid enumerates and maps items from the pg_type table.
package oid

import (
	"reflect"
	"time"
)

/*
Many sections of the code below are generated using queries into the pg_type table.  Please
see the accompanying generator.sql file for these queries.
*/

type Oid uint32
type Category byte

var BYTE_ARRRY_TYPE = reflect.TypeOf(*new([]byte))

const (
	// Insert results of first query here
	T_bool Oid             = 16
	T_bytea Oid            = 17
	T_char  Oid            = 18
	T_name Oid             = 19
	T_int8 Oid             = 20
	T_int2 Oid             = 21
	T_int2vector Oid       = 22
	T_int4 Oid             = 23
	T_regproc Oid          = 24
	T_text Oid             = 25
	T_oid Oid              = 26
	T_tid Oid              = 27
	T_xid Oid              = 28
	T_cid Oid              = 29
	T_oidvector Oid        = 30
	T_pg_type Oid          = 71
	T_pg_attribute Oid     = 75
	T_pg_proc Oid          = 81
	T_pg_class Oid         = 83
	T_json Oid             = 114
	T_xml Oid              = 142
	T__xml Oid             = 143
	T_pg_node_tree Oid     = 194
	T__json Oid            = 199
	T_smgr Oid             = 210
	T_point Oid            = 600
	T_lseg Oid             = 601
	T_path Oid             = 602
	T_box Oid              = 603
	T_polygon Oid          = 604
	T_line Oid             = 628
	T__line Oid            = 629
	T_cidr Oid             = 650
	T__cidr Oid            = 651
	T_float4 Oid           = 700
	T_float8 Oid           = 701
	T_abstime Oid          = 702
	T_reltime Oid          = 703
	T_tinterval Oid        = 704
	T_unknown Oid          = 705
	T_circle Oid           = 718
	T__circle Oid          = 719
	T_money Oid            = 790
	T__money Oid           = 791
	T_macaddr Oid          = 829
	T_inet Oid             = 869
	T__bool Oid            = 1000
	T__bytea Oid           = 1001
	T__char Oid            = 1002
	T__name Oid            = 1003
	T__int2 Oid            = 1005
	T__int2vector Oid      = 1006
	T__int4 Oid            = 1007
	T__regproc Oid         = 1008
	T__text Oid            = 1009
	T__tid Oid             = 1010
	T__xid Oid             = 1011
	T__cid Oid             = 1012
	T__oidvector Oid       = 1013
	T__bpchar Oid          = 1014
	T__varchar Oid         = 1015
	T__int8 Oid            = 1016
	T__point Oid           = 1017
	T__lseg Oid            = 1018
	T__path Oid            = 1019
	T__box Oid             = 1020
	T__float4 Oid          = 1021
	T__float8 Oid          = 1022
	T__abstime Oid         = 1023
	T__reltime Oid         = 1024
	T__tinterval Oid       = 1025
	T__polygon Oid         = 1027
	T__oid Oid             = 1028
	T_aclitem Oid          = 1033
	T__aclitem Oid         = 1034
	T__macaddr Oid         = 1040
	T__inet Oid            = 1041
	T_bpchar Oid           = 1042
	T_varchar Oid          = 1043
	T_date Oid             = 1082
	T_time Oid             = 1083
	T_timestamp Oid        = 1114
	T__timestamp Oid       = 1115
	T__date Oid            = 1182
	T__time Oid            = 1183
	T_timestamptz Oid      = 1184
	T__timestamptz Oid     = 1185
	T_interval Oid         = 1186
	T__interval Oid        = 1187
	T__numeric Oid         = 1231
	T_pg_database Oid      = 1248
	T__cstring Oid         = 1263
	T_timetz Oid           = 1266
	T__timetz Oid          = 1270
	T_bit Oid              = 1560
	T__bit Oid             = 1561
	T_varbit Oid           = 1562
	T__varbit Oid          = 1563
	T_numeric Oid          = 1700
	T_refcursor Oid        = 1790
	T__refcursor Oid       = 2201
	T_regprocedure Oid     = 2202
	T_regoper Oid          = 2203
	T_regoperator Oid      = 2204
	T_regclass Oid         = 2205
	T_regtype Oid          = 2206
	T__regprocedure Oid    = 2207
	T__regoper Oid         = 2208
	T__regoperator Oid     = 2209
	T__regclass Oid        = 2210
	T__regtype Oid         = 2211
	T_record Oid           = 2249
	T_cstring Oid          = 2275
	T_any Oid              = 2276
	T_anyarray Oid         = 2277
	T_void Oid             = 2278
	T_trigger Oid          = 2279
	T_language_handler Oid = 2280
	T_internal Oid         = 2281
	T_opaque Oid           = 2282
	T_anyelement Oid       = 2283
	T__record Oid          = 2287
	T_anynonarray Oid      = 2776
	T_pg_authid Oid        = 2842
	T_pg_auth_members Oid  = 2843
	T__txid_snapshot Oid   = 2949
	T_uuid Oid             = 2950
	T__uuid Oid            = 2951
	T_txid_snapshot Oid    = 2970
	T_fdw_handler Oid      = 3115
	T_anyenum Oid          = 3500
	T_tsvector Oid         = 3614
	T_tsquery Oid          = 3615
	T_gtsvector Oid        = 3642
	T__tsvector Oid        = 3643
	T__gtsvector Oid       = 3644
	T__tsquery Oid         = 3645
	T_regconfig Oid        = 3734
	T__regconfig Oid       = 3735
	T_regdictionary Oid    = 3769
	T__regdictionary Oid   = 3770
	T_anyrange Oid         = 3831
	T_int4range Oid        = 3904
	T__int4range Oid       = 3905
	T_numrange Oid         = 3906
	T__numrange Oid        = 3907
	T_tsrange Oid          = 3908
	T__tsrange Oid         = 3909
	T_tstzrange Oid        = 3910
	T__tstzrange Oid       = 3911
	T_daterange Oid        = 3912
	T__daterange Oid       = 3913
	T_int8range Oid        = 3926
	T__int8range Oid       = 3927
)

const (
	// values from http://www.postgresql.org/docs/9.2/static/catalog-pg-type.html
	C_array           Category = 'A'
	C_voolean         Category = 'B'
	C_composite       Category = 'C'
	C_date_time       Category = 'D'
	C_enum            Category = 'E'
	C_feometric       Category = 'G'
	C_network_address Category = 'I'
	C_numeric         Category = 'N'
	C_pseudo          Category = 'P'
	C_range           Category = 'R'
	C_string          Category = 'S'
	C_timespan        Category = 'T'
	C_user_defined    Category = 'U'
	C_bit             Category = 'V'
	C_unknown         Category = 'X'
)

var ArrayType = make(map[Oid]Oid)
var ElementType = make(map[Oid]Oid)
var category = make(map[Oid]Category)
var goTypes = make(map[Oid]reflect.Type)

// this could be done with a query too, but fudge it for now
func GetArrayElementDelimiter(typ Oid) byte {
	if typ == T_box {
		return ';'
	}

	return ','
}

func IsArray(typ Oid) bool {
	return category[typ] == C_array
}

func GetGoType(typ Oid) reflect.Type {
	t, ok := goTypes[typ]

	if ok {
		return t
	}

	return BYTE_ARRRY_TYPE
}

func init() {
	// this strikes me as fairly ridiculous.  is there a better way?
	goTypes[T_bool] = reflect.TypeOf(*new(bool))
	goTypes[T_int8] = reflect.TypeOf(*new(int64))
	goTypes[T_int4] = reflect.TypeOf(*new(int32))
	goTypes[T_int2] = reflect.TypeOf(*new(int16))
	goTypes[T_timestamptz] = reflect.TypeOf(*new(time.Time))
	goTypes[T_timestamp] = reflect.TypeOf(*new(time.Time))
	goTypes[T_time] = reflect.TypeOf(*new(time.Time))
	goTypes[T_timetz] = reflect.TypeOf(*new(time.Time))
	goTypes[T_date] = reflect.TypeOf(*new(time.Time))
	goTypes[T_float4] = reflect.TypeOf(*new(float32))
	goTypes[T_float8] = reflect.TypeOf(*new(float64))
	goTypes[T_varchar] = reflect.TypeOf(*new(string))
	goTypes[T_char] = reflect.TypeOf(*new(string))
	goTypes[T_text] = reflect.TypeOf(*new(string))

	// anything else ends up as a []byte

	// insert results of second query here
	ArrayType[T_bool] = T__bool
	ArrayType[T_bytea] = T__bytea
	ArrayType[T_char] = T__char
	ArrayType[T_name] = T__name
	ArrayType[T_int8] = T__int8
	ArrayType[T_int2] = T__int2
	ArrayType[T_int2vector] = T__int2vector
	ArrayType[T_int4] = T__int4
	ArrayType[T_regproc] = T__regproc
	ArrayType[T_text] = T__text
	ArrayType[T_oid] = T__oid
	ArrayType[T_tid] = T__tid
	ArrayType[T_xid] = T__xid
	ArrayType[T_cid] = T__cid
	ArrayType[T_oidvector] = T__oidvector
	ArrayType[T_json] = T__json
	ArrayType[T_xml] = T__xml
	ArrayType[T_point] = T__point
	ArrayType[T_lseg] = T__lseg
	ArrayType[T_path] = T__path
	ArrayType[T_box] = T__box
	ArrayType[T_polygon] = T__polygon
	ArrayType[T_line] = T__line
	ArrayType[T_cidr] = T__cidr
	ArrayType[T_float4] = T__float4
	ArrayType[T_float8] = T__float8
	ArrayType[T_abstime] = T__abstime
	ArrayType[T_reltime] = T__reltime
	ArrayType[T_tinterval] = T__tinterval
	ArrayType[T_circle] = T__circle
	ArrayType[T_money] = T__money
	ArrayType[T_macaddr] = T__macaddr
	ArrayType[T_inet] = T__inet
	ArrayType[T_aclitem] = T__aclitem
	ArrayType[T_bpchar] = T__bpchar
	ArrayType[T_varchar] = T__varchar
	ArrayType[T_date] = T__date
	ArrayType[T_time] = T__time
	ArrayType[T_timestamp] = T__timestamp
	ArrayType[T_timestamptz] = T__timestamptz
	ArrayType[T_interval] = T__interval
	ArrayType[T_timetz] = T__timetz
	ArrayType[T_bit] = T__bit
	ArrayType[T_varbit] = T__varbit
	ArrayType[T_numeric] = T__numeric
	ArrayType[T_refcursor] = T__refcursor
	ArrayType[T_regprocedure] = T__regprocedure
	ArrayType[T_regoper] = T__regoper
	ArrayType[T_regoperator] = T__regoperator
	ArrayType[T_regclass] = T__regclass
	ArrayType[T_regtype] = T__regtype
	ArrayType[T_record] = T__record
	ArrayType[T_cstring] = T__cstring
	ArrayType[T_uuid] = T__uuid
	ArrayType[T_txid_snapshot] = T__txid_snapshot
	ArrayType[T_tsvector] = T__tsvector
	ArrayType[T_tsquery] = T__tsquery
	ArrayType[T_gtsvector] = T__gtsvector
	ArrayType[T_regconfig] = T__regconfig
	ArrayType[T_regdictionary] = T__regdictionary
	ArrayType[T_int4range] = T__int4range
	ArrayType[T_numrange] = T__numrange
	ArrayType[T_tsrange] = T__tsrange
	ArrayType[T_tstzrange] = T__tstzrange
	ArrayType[T_daterange] = T__daterange
	ArrayType[T_int8range] = T__int8range


	// insert results of 3rd query here
	ElementType[T_name] = T_char
	ElementType[T_int2vector] = T_int2
	ElementType[T_oidvector] = T_oid
	ElementType[T__xml] = T_xml
	ElementType[T__json] = T_json
	ElementType[T_point] = T_float8
	ElementType[T_lseg] = T_point
	ElementType[T_box] = T_point
	ElementType[T_line] = T_float8
	ElementType[T__line] = T_line
	ElementType[T__cidr] = T_cidr
	ElementType[T__circle] = T_circle
	ElementType[T__money] = T_money
	ElementType[T__bool] = T_bool
	ElementType[T__bytea] = T_bytea
	ElementType[T__char] = T_char
	ElementType[T__name] = T_name
	ElementType[T__int2] = T_int2
	ElementType[T__int2vector] = T_int2vector
	ElementType[T__int4] = T_int4
	ElementType[T__regproc] = T_regproc
	ElementType[T__text] = T_text
	ElementType[T__tid] = T_tid
	ElementType[T__xid] = T_xid
	ElementType[T__cid] = T_cid
	ElementType[T__oidvector] = T_oidvector
	ElementType[T__bpchar] = T_bpchar
	ElementType[T__varchar] = T_varchar
	ElementType[T__int8] = T_int8
	ElementType[T__point] = T_point
	ElementType[T__lseg] = T_lseg
	ElementType[T__path] = T_path
	ElementType[T__box] = T_box
	ElementType[T__float4] = T_float4
	ElementType[T__float8] = T_float8
	ElementType[T__abstime] = T_abstime
	ElementType[T__reltime] = T_reltime
	ElementType[T__tinterval] = T_tinterval
	ElementType[T__polygon] = T_polygon
	ElementType[T__oid] = T_oid
	ElementType[T__aclitem] = T_aclitem
	ElementType[T__macaddr] = T_macaddr
	ElementType[T__inet] = T_inet
	ElementType[T__timestamp] = T_timestamp
	ElementType[T__date] = T_date
	ElementType[T__time] = T_time
	ElementType[T__timestamptz] = T_timestamptz
	ElementType[T__interval] = T_interval
	ElementType[T__numeric] = T_numeric
	ElementType[T__cstring] = T_cstring
	ElementType[T__timetz] = T_timetz
	ElementType[T__bit] = T_bit
	ElementType[T__varbit] = T_varbit
	ElementType[T__refcursor] = T_refcursor
	ElementType[T__regprocedure] = T_regprocedure
	ElementType[T__regoper] = T_regoper
	ElementType[T__regoperator] = T_regoperator
	ElementType[T__regclass] = T_regclass
	ElementType[T__regtype] = T_regtype
	ElementType[T__record] = T_record
	ElementType[T__txid_snapshot] = T_txid_snapshot
	ElementType[T__uuid] = T_uuid
	ElementType[T__tsvector] = T_tsvector
	ElementType[T__gtsvector] = T_gtsvector
	ElementType[T__tsquery] = T_tsquery
	ElementType[T__regconfig] = T_regconfig
	ElementType[T__regdictionary] = T_regdictionary
	ElementType[T__int4range] = T_int4range
	ElementType[T__numrange] = T_numrange
	ElementType[T__tsrange] = T_tsrange
	ElementType[T__tstzrange] = T_tstzrange
	ElementType[T__daterange] = T_daterange
	ElementType[T__int8range] = T_int8range


	// results of the 4th query go here
	category[T_bool] = 'B'
	category[T_bytea] = 'U'
	category[T_char] = 'S'
	category[T_name] = 'S'
	category[T_int8] = 'N'
	category[T_int2] = 'N'
	category[T_int2vector] = 'A'
	category[T_int4] = 'N'
	category[T_regproc] = 'N'
	category[T_text] = 'S'
	category[T_oid] = 'N'
	category[T_tid] = 'U'
	category[T_xid] = 'U'
	category[T_cid] = 'U'
	category[T_oidvector] = 'A'
	category[T_pg_type] = 'C'
	category[T_pg_attribute] = 'C'
	category[T_pg_proc] = 'C'
	category[T_pg_class] = 'C'
	category[T_json] = 'U'
	category[T_xml] = 'U'
	category[T__xml] = 'A'
	category[T_pg_node_tree] = 'S'
	category[T__json] = 'A'
	category[T_smgr] = 'U'
	category[T_point] = 'G'
	category[T_lseg] = 'G'
	category[T_path] = 'G'
	category[T_box] = 'G'
	category[T_polygon] = 'G'
	category[T_line] = 'G'
	category[T__line] = 'A'
	category[T_cidr] = 'I'
	category[T__cidr] = 'A'
	category[T_float4] = 'N'
	category[T_float8] = 'N'
	category[T_abstime] = 'D'
	category[T_reltime] = 'T'
	category[T_tinterval] = 'T'
	category[T_unknown] = 'X'
	category[T_circle] = 'G'
	category[T__circle] = 'A'
	category[T_money] = 'N'
	category[T__money] = 'A'
	category[T_macaddr] = 'U'
	category[T_inet] = 'I'
	category[T__bool] = 'A'
	category[T__bytea] = 'A'
	category[T__char] = 'A'
	category[T__name] = 'A'
	category[T__int2] = 'A'
	category[T__int2vector] = 'A'
	category[T__int4] = 'A'
	category[T__regproc] = 'A'
	category[T__text] = 'A'
	category[T__tid] = 'A'
	category[T__xid] = 'A'
	category[T__cid] = 'A'
	category[T__oidvector] = 'A'
	category[T__bpchar] = 'A'
	category[T__varchar] = 'A'
	category[T__int8] = 'A'
	category[T__point] = 'A'
	category[T__lseg] = 'A'
	category[T__path] = 'A'
	category[T__box] = 'A'
	category[T__float4] = 'A'
	category[T__float8] = 'A'
	category[T__abstime] = 'A'
	category[T__reltime] = 'A'
	category[T__tinterval] = 'A'
	category[T__polygon] = 'A'
	category[T__oid] = 'A'
	category[T_aclitem] = 'U'
	category[T__aclitem] = 'A'
	category[T__macaddr] = 'A'
	category[T__inet] = 'A'
	category[T_bpchar] = 'S'
	category[T_varchar] = 'S'
	category[T_date] = 'D'
	category[T_time] = 'D'
	category[T_timestamp] = 'D'
	category[T__timestamp] = 'A'
	category[T__date] = 'A'
	category[T__time] = 'A'
	category[T_timestamptz] = 'D'
	category[T__timestamptz] = 'A'
	category[T_interval] = 'T'
	category[T__interval] = 'A'
	category[T__numeric] = 'A'
	category[T_pg_database] = 'C'
	category[T__cstring] = 'A'
	category[T_timetz] = 'D'
	category[T__timetz] = 'A'
	category[T_bit] = 'V'
	category[T__bit] = 'A'
	category[T_varbit] = 'V'
	category[T__varbit] = 'A'
	category[T_numeric] = 'N'
	category[T_refcursor] = 'U'
	category[T__refcursor] = 'A'
	category[T_regprocedure] = 'N'
	category[T_regoper] = 'N'
	category[T_regoperator] = 'N'
	category[T_regclass] = 'N'
	category[T_regtype] = 'N'
	category[T__regprocedure] = 'A'
	category[T__regoper] = 'A'
	category[T__regoperator] = 'A'
	category[T__regclass] = 'A'
	category[T__regtype] = 'A'
	category[T_record] = 'P'
	category[T_cstring] = 'P'
	category[T_any] = 'P'
	category[T_anyarray] = 'P'
	category[T_void] = 'P'
	category[T_trigger] = 'P'
	category[T_language_handler] = 'P'
	category[T_internal] = 'P'
	category[T_opaque] = 'P'
	category[T_anyelement] = 'P'
	category[T__record] = 'P'
	category[T_anynonarray] = 'P'
	category[T_pg_authid] = 'C'
	category[T_pg_auth_members] = 'C'
	category[T__txid_snapshot] = 'A'
	category[T_uuid] = 'U'
	category[T__uuid] = 'A'
	category[T_txid_snapshot] = 'U'
	category[T_fdw_handler] = 'P'
	category[T_anyenum] = 'P'
	category[T_tsvector] = 'U'
	category[T_tsquery] = 'U'
	category[T_gtsvector] = 'U'
	category[T__tsvector] = 'A'
	category[T__gtsvector] = 'A'
	category[T__tsquery] = 'A'
	category[T_regconfig] = 'N'
	category[T__regconfig] = 'A'
	category[T_regdictionary] = 'N'
	category[T__regdictionary] = 'A'
	category[T_anyrange] = 'P'
	category[T_int4range] = 'R'
	category[T__int4range] = 'A'
	category[T_numrange] = 'R'
	category[T__numrange] = 'A'
	category[T_tsrange] = 'R'
	category[T__tsrange] = 'A'
	category[T_tstzrange] = 'R'
	category[T__tstzrange] = 'A'
	category[T_daterange] = 'R'
	category[T__daterange] = 'A'
	category[T_int8range] = 'R'
	category[T__int8range] = 'A'

}
