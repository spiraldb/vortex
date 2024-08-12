window.BENCHMARK_DATA = {
  "lastUpdate": 1723487407863,
  "repoUrl": "https://github.com/spiraldb/vortex",
  "entries": {
    "Vortex benchmarks": [
      {
        "commit": {
          "author": {
            "email": "github@robertk.io",
            "name": "Robert Kruszewski",
            "username": "robert3005"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "ff952d1f0cb98ce86a29b6190d4a03897c5e9a6a",
          "message": "Use github benchmark action to run benchmarks (#602)",
          "timestamp": "2024-08-12T18:55:34+01:00",
          "tree_id": "71e495c3dad319d41af0290a2595728cb201f652",
          "url": "https://github.com/spiraldb/vortex/commit/ff952d1f0cb98ce86a29b6190d4a03897c5e9a6a"
        },
        "date": 1723487407503,
        "tool": "cargo",
        "benches": [
          {
            "name": "tpch_q1/vortex-pushdown-disabled",
            "value": 487559047,
            "range": "± 1483300",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-pushdown-enabled",
            "value": 488090253,
            "range": "± 5267085",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/arrow",
            "value": 472612974,
            "range": "± 2443572",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/parquet",
            "value": 679261427,
            "range": "± 4659632",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/persistent_compressed_vortex",
            "value": 807557368,
            "range": "± 8306557",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/persistent_uncompressed_vortex",
            "value": 647397826,
            "range": "± 3106081",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-pushdown-disabled",
            "value": 148903767,
            "range": "± 1588862",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-pushdown-enabled",
            "value": 175749838,
            "range": "± 1915186",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/arrow",
            "value": 127587064,
            "range": "± 504890",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/parquet",
            "value": 165037829,
            "range": "± 2319604",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/persistent_compressed_vortex",
            "value": 168702059,
            "range": "± 839108",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/persistent_uncompressed_vortex",
            "value": 154249213,
            "range": "± 517897",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-pushdown-disabled",
            "value": 161446795,
            "range": "± 661426",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-pushdown-enabled",
            "value": 210942197,
            "range": "± 2123648",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/arrow",
            "value": 155759592,
            "range": "± 510573",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/parquet",
            "value": 345379648,
            "range": "± 3611169",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/persistent_compressed_vortex",
            "value": 259165673,
            "range": "± 3974595",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/persistent_uncompressed_vortex",
            "value": 200586197,
            "range": "± 2249999",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-pushdown-disabled",
            "value": 129311414,
            "range": "± 1654434",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-pushdown-enabled",
            "value": 127414180,
            "range": "± 1083217",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/arrow",
            "value": 123114055,
            "range": "± 1241745",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/parquet",
            "value": 234766095,
            "range": "± 2065625",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/persistent_compressed_vortex",
            "value": 254032523,
            "range": "± 4098520",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/persistent_uncompressed_vortex",
            "value": 203871244,
            "range": "± 3004983",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-pushdown-disabled",
            "value": 329119911,
            "range": "± 4252630",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-pushdown-enabled",
            "value": 319122811,
            "range": "± 2375063",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/arrow",
            "value": 309912646,
            "range": "± 3968235",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/parquet",
            "value": 487444995,
            "range": "± 3759604",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/persistent_compressed_vortex",
            "value": 430500097,
            "range": "± 7572919",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/persistent_uncompressed_vortex",
            "value": 377377479,
            "range": "± 4759371",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-pushdown-disabled",
            "value": 41663471,
            "range": "± 899324",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-pushdown-enabled",
            "value": 178877524,
            "range": "± 1168020",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/arrow",
            "value": 37489474,
            "range": "± 511467",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/parquet",
            "value": 154349967,
            "range": "± 3520457",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/persistent_compressed_vortex",
            "value": 128583876,
            "range": "± 1086799",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/persistent_uncompressed_vortex",
            "value": 101381003,
            "range": "± 837835",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-pushdown-disabled",
            "value": 594364006,
            "range": "± 17330413",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-pushdown-enabled",
            "value": 726518975,
            "range": "± 16343696",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/arrow",
            "value": 574326530,
            "range": "± 13392241",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/parquet",
            "value": 760368536,
            "range": "± 13119115",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/persistent_compressed_vortex",
            "value": 723261072,
            "range": "± 4985392",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/persistent_uncompressed_vortex",
            "value": 671723752,
            "range": "± 7999873",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-pushdown-disabled",
            "value": 234996219,
            "range": "± 1374561",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-pushdown-enabled",
            "value": 2056179452,
            "range": "± 22558441",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/arrow",
            "value": 228829825,
            "range": "± 3046171",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/parquet",
            "value": 493282602,
            "range": "± 3171381",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/persistent_compressed_vortex",
            "value": 334244601,
            "range": "± 2556369",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/persistent_uncompressed_vortex",
            "value": 275244775,
            "range": "± 3170973",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-pushdown-disabled",
            "value": 449923906,
            "range": "± 9574812",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-pushdown-enabled",
            "value": 455784937,
            "range": "± 11550452",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/arrow",
            "value": 433024880,
            "range": "± 7005137",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/parquet",
            "value": 726817440,
            "range": "± 5483282",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/persistent_compressed_vortex",
            "value": 580021017,
            "range": "± 6575420",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/persistent_uncompressed_vortex",
            "value": 491784145,
            "range": "± 8761146",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-pushdown-disabled",
            "value": 242299005,
            "range": "± 1666459",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-pushdown-enabled",
            "value": 336745686,
            "range": "± 3359666",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/arrow",
            "value": 233579538,
            "range": "± 1839251",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/parquet",
            "value": 483126053,
            "range": "± 2971413",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/persistent_compressed_vortex",
            "value": 494460084,
            "range": "± 3382106",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/persistent_uncompressed_vortex",
            "value": 346171889,
            "range": "± 3576281",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-pushdown-disabled",
            "value": 225995739,
            "range": "± 3480739",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-pushdown-enabled",
            "value": 642017747,
            "range": "± 11354998",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/arrow",
            "value": 183493695,
            "range": "± 925007",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/parquet",
            "value": 201015806,
            "range": "± 3241273",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/persistent_compressed_vortex",
            "value": 175386263,
            "range": "± 1794761",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/persistent_uncompressed_vortex",
            "value": 170913032,
            "range": "± 3641149",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-pushdown-disabled",
            "value": 185244909,
            "range": "± 1190918",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-pushdown-enabled",
            "value": 226566655,
            "range": "± 1375927",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/arrow",
            "value": 172227100,
            "range": "± 720233",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/parquet",
            "value": 362273152,
            "range": "± 3808805",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/persistent_compressed_vortex",
            "value": 443343823,
            "range": "± 2309959",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/persistent_uncompressed_vortex",
            "value": 334895469,
            "range": "± 5124297",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-pushdown-disabled",
            "value": 309935599,
            "range": "± 10808821",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-pushdown-enabled",
            "value": 318098839,
            "range": "± 10061600",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/arrow",
            "value": 321981275,
            "range": "± 9065526",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/parquet",
            "value": 458786808,
            "range": "± 4514320",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/persistent_compressed_vortex",
            "value": 350696860,
            "range": "± 2849857",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/persistent_uncompressed_vortex",
            "value": 345924474,
            "range": "± 1865451",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-pushdown-disabled",
            "value": 39708666,
            "range": "± 1007165",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-pushdown-enabled",
            "value": 40871177,
            "range": "± 645258",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/arrow",
            "value": 39278425,
            "range": "± 391866",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/parquet",
            "value": 222911980,
            "range": "± 722377",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/persistent_compressed_vortex",
            "value": 129231304,
            "range": "± 2142439",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/persistent_uncompressed_vortex",
            "value": 106097416,
            "range": "± 1648094",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-pushdown-disabled",
            "value": 124958831,
            "range": "± 613890",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-pushdown-enabled",
            "value": 44126122,
            "range": "± 354115",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/arrow",
            "value": 111694838,
            "range": "± 569761",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/parquet",
            "value": 128643174,
            "range": "± 673486",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/persistent_compressed_vortex",
            "value": 145431074,
            "range": "± 477611",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/persistent_uncompressed_vortex",
            "value": 138119129,
            "range": "± 512878",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-pushdown-disabled",
            "value": 627146634,
            "range": "± 9864915",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-pushdown-enabled",
            "value": 1140282443,
            "range": "± 4089177",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/arrow",
            "value": 537364395,
            "range": "± 11943786",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/parquet",
            "value": 596912630,
            "range": "± 6902170",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/persistent_compressed_vortex",
            "value": 679597700,
            "range": "± 7212959",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/persistent_uncompressed_vortex",
            "value": 645894956,
            "range": "± 4965094",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-pushdown-disabled",
            "value": 1116231293,
            "range": "± 25846752",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-pushdown-enabled",
            "value": 1095197953,
            "range": "± 12286234",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/arrow",
            "value": 1092617634,
            "range": "± 29621351",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/parquet",
            "value": 1323117422,
            "range": "± 42177675",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/persistent_compressed_vortex",
            "value": 1317212904,
            "range": "± 35197417",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/persistent_uncompressed_vortex",
            "value": 1166026320,
            "range": "± 4326538",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-pushdown-disabled",
            "value": 174015150,
            "range": "± 944266",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-pushdown-enabled",
            "value": 514806323,
            "range": "± 2128667",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/arrow",
            "value": 158329726,
            "range": "± 728908",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/parquet",
            "value": 481018991,
            "range": "± 6870091",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/persistent_compressed_vortex",
            "value": 1254090769,
            "range": "± 8576109",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/persistent_uncompressed_vortex",
            "value": 793823654,
            "range": "± 1796087",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-pushdown-disabled",
            "value": 264985714,
            "range": "± 3718666",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-pushdown-enabled",
            "value": 269293162,
            "range": "± 3377393",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/arrow",
            "value": 255729786,
            "range": "± 9516718",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/parquet",
            "value": 368352596,
            "range": "± 5219037",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/persistent_compressed_vortex",
            "value": 383789338,
            "range": "± 8373065",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/persistent_uncompressed_vortex",
            "value": 385370923,
            "range": "± 5588703",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-pushdown-disabled",
            "value": 978835234,
            "range": "± 14283714",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-pushdown-enabled",
            "value": 1663474561,
            "range": "± 15461993",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/arrow",
            "value": 942755684,
            "range": "± 8652262",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/parquet",
            "value": 1127785132,
            "range": "± 15938362",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/persistent_compressed_vortex",
            "value": 939184393,
            "range": "± 5163340",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/persistent_uncompressed_vortex",
            "value": 814542000,
            "range": "± 24118067",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-pushdown-disabled",
            "value": 98034441,
            "range": "± 792426",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-pushdown-enabled",
            "value": 100560977,
            "range": "± 638231",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/arrow",
            "value": 67760156,
            "range": "± 525653",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/parquet",
            "value": 96979719,
            "range": "± 997211",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/persistent_compressed_vortex",
            "value": 115584678,
            "range": "± 754401",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/persistent_uncompressed_vortex",
            "value": 114674778,
            "range": "± 944665",
            "unit": "ns/iter"
          }
        ]
      }
    ]
  }
}