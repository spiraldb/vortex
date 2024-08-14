window.BENCHMARK_DATA = {
  "lastUpdate": 1723640225346,
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
          "id": "e88536b861a0bdebbdc005ed0a0b3f806c228077",
          "message": "Better names for tpch benchmark series (#605)",
          "timestamp": "2024-08-13T00:39:43+01:00",
          "tree_id": "e7f72dec41548738ff1f6426c25bc87ec3d3fb46",
          "url": "https://github.com/spiraldb/vortex/commit/e88536b861a0bdebbdc005ed0a0b3f806c228077"
        },
        "date": 1723507795889,
        "tool": "cargo",
        "benches": [
          {
            "name": "tpch_q1/vortex-in-memory-no-pushdown",
            "value": 484993833,
            "range": "± 7298071",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-in-memory-pushdown",
            "value": 478322839,
            "range": "± 2220997",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/arrow",
            "value": 465400983,
            "range": "± 3044552",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/parquet",
            "value": 676528521,
            "range": "± 2474265",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-file-compressed",
            "value": 788538667,
            "range": "± 11065727",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-file-uncompressed",
            "value": 643271907,
            "range": "± 2263838",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-in-memory-no-pushdown",
            "value": 150394862,
            "range": "± 1067015",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-in-memory-pushdown",
            "value": 181733571,
            "range": "± 1279485",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/arrow",
            "value": 130204810,
            "range": "± 661840",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/parquet",
            "value": 167404690,
            "range": "± 948121",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-file-compressed",
            "value": 172421640,
            "range": "± 2948838",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-file-uncompressed",
            "value": 156638803,
            "range": "± 2886634",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-in-memory-no-pushdown",
            "value": 162739687,
            "range": "± 1756933",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-in-memory-pushdown",
            "value": 212454756,
            "range": "± 1299266",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/arrow",
            "value": 160032448,
            "range": "± 768228",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/parquet",
            "value": 348992428,
            "range": "± 2305026",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-file-compressed",
            "value": 254526883,
            "range": "± 978412",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-file-uncompressed",
            "value": 197106738,
            "range": "± 1962109",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-in-memory-no-pushdown",
            "value": 128109702,
            "range": "± 1331233",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-in-memory-pushdown",
            "value": 130186365,
            "range": "± 1195231",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/arrow",
            "value": 122972740,
            "range": "± 779419",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/parquet",
            "value": 240723301,
            "range": "± 2352659",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-file-compressed",
            "value": 250179799,
            "range": "± 3139571",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-file-uncompressed",
            "value": 200518821,
            "range": "± 4014159",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-in-memory-no-pushdown",
            "value": 313859960,
            "range": "± 3097982",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-in-memory-pushdown",
            "value": 315563932,
            "range": "± 1645743",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/arrow",
            "value": 310639838,
            "range": "± 688176",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/parquet",
            "value": 481799625,
            "range": "± 5055810",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-file-compressed",
            "value": 411317302,
            "range": "± 2888680",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-file-uncompressed",
            "value": 356851190,
            "range": "± 4269067",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-in-memory-no-pushdown",
            "value": 40884226,
            "range": "± 265662",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-in-memory-pushdown",
            "value": 175904851,
            "range": "± 4033098",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/arrow",
            "value": 37850874,
            "range": "± 508176",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/parquet",
            "value": 150557514,
            "range": "± 526461",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-file-compressed",
            "value": 125517869,
            "range": "± 2400502",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-file-uncompressed",
            "value": 98519911,
            "range": "± 2190531",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-in-memory-no-pushdown",
            "value": 575555276,
            "range": "± 5707633",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-in-memory-pushdown",
            "value": 730107915,
            "range": "± 9766628",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/arrow",
            "value": 585312404,
            "range": "± 6823668",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/parquet",
            "value": 749686203,
            "range": "± 7798715",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-file-compressed",
            "value": 707686427,
            "range": "± 7707099",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-file-uncompressed",
            "value": 655384808,
            "range": "± 5944333",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-in-memory-no-pushdown",
            "value": 224103961,
            "range": "± 1233456",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-in-memory-pushdown",
            "value": 2088190264,
            "range": "± 37250556",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/arrow",
            "value": 224721285,
            "range": "± 1694528",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/parquet",
            "value": 492713503,
            "range": "± 1836651",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-file-compressed",
            "value": 325705154,
            "range": "± 4429042",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-file-uncompressed",
            "value": 255768999,
            "range": "± 4998922",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-in-memory-no-pushdown",
            "value": 430334224,
            "range": "± 7454772",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-in-memory-pushdown",
            "value": 429530364,
            "range": "± 4322497",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/arrow",
            "value": 429490332,
            "range": "± 11735423",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/parquet",
            "value": 719428652,
            "range": "± 6423382",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-file-compressed",
            "value": 558099974,
            "range": "± 4729654",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-file-uncompressed",
            "value": 493787066,
            "range": "± 3035828",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-in-memory-no-pushdown",
            "value": 244179015,
            "range": "± 964568",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-in-memory-pushdown",
            "value": 342073581,
            "range": "± 2426447",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/arrow",
            "value": 243976287,
            "range": "± 3570352",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/parquet",
            "value": 501352032,
            "range": "± 3023843",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-file-compressed",
            "value": 483283805,
            "range": "± 1137533",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-file-uncompressed",
            "value": 345342161,
            "range": "± 1356813",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-in-memory-no-pushdown",
            "value": 222632814,
            "range": "± 2969040",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-in-memory-pushdown",
            "value": 625541738,
            "range": "± 4131642",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/arrow",
            "value": 179890701,
            "range": "± 808700",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/parquet",
            "value": 197234643,
            "range": "± 1713259",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-file-compressed",
            "value": 177363567,
            "range": "± 1999587",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-file-uncompressed",
            "value": 175244728,
            "range": "± 3490343",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-in-memory-no-pushdown",
            "value": 185462415,
            "range": "± 1041323",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-in-memory-pushdown",
            "value": 231842590,
            "range": "± 2744356",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/arrow",
            "value": 173355418,
            "range": "± 687436",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/parquet",
            "value": 365909706,
            "range": "± 3284224",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-file-compressed",
            "value": 448922582,
            "range": "± 5034001",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-file-uncompressed",
            "value": 336478281,
            "range": "± 5226809",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-in-memory-no-pushdown",
            "value": 345597216,
            "range": "± 2949211",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-in-memory-pushdown",
            "value": 345870840,
            "range": "± 3822297",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/arrow",
            "value": 340313935,
            "range": "± 3806986",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/parquet",
            "value": 496747427,
            "range": "± 5487223",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-file-compressed",
            "value": 370197174,
            "range": "± 4448703",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-file-uncompressed",
            "value": 374647378,
            "range": "± 4904665",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-in-memory-no-pushdown",
            "value": 40509051,
            "range": "± 809648",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-in-memory-pushdown",
            "value": 41172707,
            "range": "± 483277",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/arrow",
            "value": 42666732,
            "range": "± 506205",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/parquet",
            "value": 224146959,
            "range": "± 2182775",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-file-compressed",
            "value": 129936869,
            "range": "± 713940",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-file-uncompressed",
            "value": 104265357,
            "range": "± 654473",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-in-memory-no-pushdown",
            "value": 125748175,
            "range": "± 595538",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-in-memory-pushdown",
            "value": 45699235,
            "range": "± 312515",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/arrow",
            "value": 112906128,
            "range": "± 445812",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/parquet",
            "value": 131020519,
            "range": "± 696882",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-file-compressed",
            "value": 148100695,
            "range": "± 1231865",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-file-uncompressed",
            "value": 141955131,
            "range": "± 581265",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-in-memory-no-pushdown",
            "value": 671509455,
            "range": "± 12532262",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-in-memory-pushdown",
            "value": 1269496423,
            "range": "± 21239190",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/arrow",
            "value": 581535844,
            "range": "± 15120479",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/parquet",
            "value": 604487336,
            "range": "± 4321710",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-file-compressed",
            "value": 722535176,
            "range": "± 10373950",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-file-uncompressed",
            "value": 678636942,
            "range": "± 11581531",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-in-memory-no-pushdown",
            "value": 1175145344,
            "range": "± 13063293",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-in-memory-pushdown",
            "value": 1106697341,
            "range": "± 17214474",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/arrow",
            "value": 1158784245,
            "range": "± 36319460",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/parquet",
            "value": 1275512731,
            "range": "± 13968717",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-file-compressed",
            "value": 1307081893,
            "range": "± 36269287",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-file-uncompressed",
            "value": 1171296886,
            "range": "± 28494136",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-in-memory-no-pushdown",
            "value": 174344368,
            "range": "± 848482",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-in-memory-pushdown",
            "value": 515914589,
            "range": "± 6242309",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/arrow",
            "value": 159761389,
            "range": "± 464674",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/parquet",
            "value": 482419101,
            "range": "± 4677943",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-file-compressed",
            "value": 1260686332,
            "range": "± 5919756",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-file-uncompressed",
            "value": 803518793,
            "range": "± 5804169",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-in-memory-no-pushdown",
            "value": 278545913,
            "range": "± 8385994",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-in-memory-pushdown",
            "value": 270577333,
            "range": "± 2275951",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/arrow",
            "value": 262609654,
            "range": "± 4074900",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/parquet",
            "value": 381979743,
            "range": "± 5429284",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-file-compressed",
            "value": 374843723,
            "range": "± 2575881",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-file-uncompressed",
            "value": 357029686,
            "range": "± 4093678",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-in-memory-no-pushdown",
            "value": 947163728,
            "range": "± 7925687",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-in-memory-pushdown",
            "value": 1659962631,
            "range": "± 26606628",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/arrow",
            "value": 965868713,
            "range": "± 13683792",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/parquet",
            "value": 1134186008,
            "range": "± 5730447",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-file-compressed",
            "value": 949860178,
            "range": "± 7828189",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-file-uncompressed",
            "value": 813705889,
            "range": "± 4151026",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-in-memory-no-pushdown",
            "value": 98107153,
            "range": "± 388824",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-in-memory-pushdown",
            "value": 99083315,
            "range": "± 434424",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/arrow",
            "value": 68434512,
            "range": "± 306853",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/parquet",
            "value": 98176183,
            "range": "± 1336756",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-file-compressed",
            "value": 115547486,
            "range": "± 1272708",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-file-uncompressed",
            "value": 112719776,
            "range": "± 1962687",
            "unit": "ns/iter"
          }
        ]
      },
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
          "id": "9c6d921d7a2d4ed5edb36cd47e4c5fe77bd4b2d8",
          "message": "Nulls as false respects original array nullability (#606)",
          "timestamp": "2024-08-13T09:56:37Z",
          "tree_id": "eb8c77f958ced42492aab24a5dd7394eb5bce115",
          "url": "https://github.com/spiraldb/vortex/commit/9c6d921d7a2d4ed5edb36cd47e4c5fe77bd4b2d8"
        },
        "date": 1723544862765,
        "tool": "cargo",
        "benches": [
          {
            "name": "tpch_q1/vortex-in-memory-no-pushdown",
            "value": 472667519,
            "range": "± 737813",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-in-memory-pushdown",
            "value": 472576457,
            "range": "± 2657349",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/arrow",
            "value": 463395853,
            "range": "± 3633502",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/parquet",
            "value": 669165786,
            "range": "± 3050418",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-file-compressed",
            "value": 772970800,
            "range": "± 3653004",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-file-uncompressed",
            "value": 641267030,
            "range": "± 4517119",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-in-memory-no-pushdown",
            "value": 147083261,
            "range": "± 876351",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-in-memory-pushdown",
            "value": 174194901,
            "range": "± 2201073",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/arrow",
            "value": 128303403,
            "range": "± 656935",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/parquet",
            "value": 160005341,
            "range": "± 426931",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-file-compressed",
            "value": 163239288,
            "range": "± 1261478",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-file-uncompressed",
            "value": 152432364,
            "range": "± 697813",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-in-memory-no-pushdown",
            "value": 158900868,
            "range": "± 628357",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-in-memory-pushdown",
            "value": 209394409,
            "range": "± 657640",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/arrow",
            "value": 155018406,
            "range": "± 368599",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/parquet",
            "value": 339372707,
            "range": "± 1077211",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-file-compressed",
            "value": 245318460,
            "range": "± 2823789",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-file-uncompressed",
            "value": 195895305,
            "range": "± 3074680",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-in-memory-no-pushdown",
            "value": 126126417,
            "range": "± 904832",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-in-memory-pushdown",
            "value": 131272806,
            "range": "± 3122582",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/arrow",
            "value": 121364493,
            "range": "± 368365",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/parquet",
            "value": 230722063,
            "range": "± 546397",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-file-compressed",
            "value": 243473596,
            "range": "± 5430292",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-file-uncompressed",
            "value": 198307653,
            "range": "± 1125966",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-in-memory-no-pushdown",
            "value": 303522273,
            "range": "± 1577007",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-in-memory-pushdown",
            "value": 307281427,
            "range": "± 1145445",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/arrow",
            "value": 302344224,
            "range": "± 2035589",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/parquet",
            "value": 454096134,
            "range": "± 5629442",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-file-compressed",
            "value": 399549032,
            "range": "± 1698528",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-file-uncompressed",
            "value": 351259224,
            "range": "± 10187030",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-in-memory-no-pushdown",
            "value": 39275017,
            "range": "± 242376",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-in-memory-pushdown",
            "value": 178232489,
            "range": "± 3557975",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/arrow",
            "value": 36916951,
            "range": "± 613586",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/parquet",
            "value": 151036836,
            "range": "± 644181",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-file-compressed",
            "value": 125158480,
            "range": "± 2501756",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-file-uncompressed",
            "value": 97162466,
            "range": "± 2221613",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-in-memory-no-pushdown",
            "value": 568914383,
            "range": "± 1559817",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-in-memory-pushdown",
            "value": 735378072,
            "range": "± 14203355",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/arrow",
            "value": 567088960,
            "range": "± 1190010",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/parquet",
            "value": 728522086,
            "range": "± 3197583",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-file-compressed",
            "value": 706529143,
            "range": "± 12109463",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-file-uncompressed",
            "value": 660194331,
            "range": "± 14793652",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-in-memory-no-pushdown",
            "value": 225978217,
            "range": "± 6781702",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-in-memory-pushdown",
            "value": 2053740319,
            "range": "± 41427653",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/arrow",
            "value": 222222268,
            "range": "± 3215555",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/parquet",
            "value": 483789414,
            "range": "± 1349000",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-file-compressed",
            "value": 324360613,
            "range": "± 2232776",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-file-uncompressed",
            "value": 269044934,
            "range": "± 1926179",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-in-memory-no-pushdown",
            "value": 415202314,
            "range": "± 969469",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-in-memory-pushdown",
            "value": 420385777,
            "range": "± 9005824",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/arrow",
            "value": 423774544,
            "range": "± 2609062",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/parquet",
            "value": 721341112,
            "range": "± 5836630",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-file-compressed",
            "value": 566771665,
            "range": "± 13206760",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-file-uncompressed",
            "value": 508726081,
            "range": "± 9697925",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-in-memory-no-pushdown",
            "value": 253842651,
            "range": "± 5145387",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-in-memory-pushdown",
            "value": 345494780,
            "range": "± 2437875",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/arrow",
            "value": 249709173,
            "range": "± 4636326",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/parquet",
            "value": 519005351,
            "range": "± 4278238",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-file-compressed",
            "value": 496925106,
            "range": "± 5248952",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-file-uncompressed",
            "value": 342351084,
            "range": "± 2208096",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-in-memory-no-pushdown",
            "value": 218802422,
            "range": "± 11288790",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-in-memory-pushdown",
            "value": 631278974,
            "range": "± 9198822",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/arrow",
            "value": 184132637,
            "range": "± 2850780",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/parquet",
            "value": 202067225,
            "range": "± 4413857",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-file-compressed",
            "value": 179524334,
            "range": "± 2705153",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-file-uncompressed",
            "value": 182790079,
            "range": "± 3424205",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-in-memory-no-pushdown",
            "value": 188394870,
            "range": "± 2742835",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-in-memory-pushdown",
            "value": 231677072,
            "range": "± 2085482",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/arrow",
            "value": 176181355,
            "range": "± 2539355",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/parquet",
            "value": 381605710,
            "range": "± 2005255",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-file-compressed",
            "value": 464729772,
            "range": "± 5428751",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-file-uncompressed",
            "value": 357521475,
            "range": "± 2758935",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-in-memory-no-pushdown",
            "value": 353006817,
            "range": "± 3824338",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-in-memory-pushdown",
            "value": 354744105,
            "range": "± 3272231",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/arrow",
            "value": 346010415,
            "range": "± 9420832",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/parquet",
            "value": 494571963,
            "range": "± 16374835",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-file-compressed",
            "value": 358480918,
            "range": "± 3173794",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-file-uncompressed",
            "value": 353404288,
            "range": "± 3107757",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-in-memory-no-pushdown",
            "value": 41304615,
            "range": "± 1312377",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-in-memory-pushdown",
            "value": 42732640,
            "range": "± 988531",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/arrow",
            "value": 40970654,
            "range": "± 1633910",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/parquet",
            "value": 223784492,
            "range": "± 1522766",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-file-compressed",
            "value": 126989504,
            "range": "± 497468",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-file-uncompressed",
            "value": 103946567,
            "range": "± 1103890",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-in-memory-no-pushdown",
            "value": 123957648,
            "range": "± 258737",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-in-memory-pushdown",
            "value": 45315314,
            "range": "± 660595",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/arrow",
            "value": 110453189,
            "range": "± 3173496",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/parquet",
            "value": 127515573,
            "range": "± 2625769",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-file-compressed",
            "value": 145137769,
            "range": "± 875018",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-file-uncompressed",
            "value": 138905687,
            "range": "± 1183216",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-in-memory-no-pushdown",
            "value": 612969115,
            "range": "± 19554919",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-in-memory-pushdown",
            "value": 1174227782,
            "range": "± 74448199",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/arrow",
            "value": 512924692,
            "range": "± 5980562",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/parquet",
            "value": 588271376,
            "range": "± 10100150",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-file-compressed",
            "value": 686850961,
            "range": "± 5989257",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-file-uncompressed",
            "value": 653925958,
            "range": "± 5801048",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-in-memory-no-pushdown",
            "value": 1050520123,
            "range": "± 16555000",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-in-memory-pushdown",
            "value": 1051902031,
            "range": "± 21721936",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/arrow",
            "value": 1035870232,
            "range": "± 7954252",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/parquet",
            "value": 1211986224,
            "range": "± 4330816",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-file-compressed",
            "value": 1222460797,
            "range": "± 20020422",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-file-uncompressed",
            "value": 1191373701,
            "range": "± 16819966",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-in-memory-no-pushdown",
            "value": 171138999,
            "range": "± 760350",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-in-memory-pushdown",
            "value": 504086110,
            "range": "± 3901982",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/arrow",
            "value": 156791160,
            "range": "± 726236",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/parquet",
            "value": 471149762,
            "range": "± 981814",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-file-compressed",
            "value": 1222406735,
            "range": "± 12120774",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-file-uncompressed",
            "value": 787464240,
            "range": "± 2475696",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-in-memory-no-pushdown",
            "value": 269860291,
            "range": "± 1838473",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-in-memory-pushdown",
            "value": 272836239,
            "range": "± 1774204",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/arrow",
            "value": 253994564,
            "range": "± 1004123",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/parquet",
            "value": 367651994,
            "range": "± 2931763",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-file-compressed",
            "value": 373227752,
            "range": "± 2057322",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-file-uncompressed",
            "value": 360287202,
            "range": "± 1666149",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-in-memory-no-pushdown",
            "value": 914117169,
            "range": "± 3982292",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-in-memory-pushdown",
            "value": 1586993926,
            "range": "± 15851645",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/arrow",
            "value": 905192784,
            "range": "± 19573122",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/parquet",
            "value": 1048954290,
            "range": "± 4557925",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-file-compressed",
            "value": 945577153,
            "range": "± 19936425",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-file-uncompressed",
            "value": 801774626,
            "range": "± 11975167",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-in-memory-no-pushdown",
            "value": 95108677,
            "range": "± 282245",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-in-memory-pushdown",
            "value": 96374259,
            "range": "± 240818",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/arrow",
            "value": 66185946,
            "range": "± 281262",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/parquet",
            "value": 94156040,
            "range": "± 413024",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-file-compressed",
            "value": 111056359,
            "range": "± 605088",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-file-uncompressed",
            "value": 110384414,
            "range": "± 532505",
            "unit": "ns/iter"
          }
        ]
      },
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
          "id": "3d93049d499001fbbb01892534f4045b403f2d04",
          "message": "RunEnd array scalar_at respects validity (#608)",
          "timestamp": "2024-08-13T11:58:37+01:00",
          "tree_id": "ee720fe52e3c828500cbe83c2086550d1cdf978f",
          "url": "https://github.com/spiraldb/vortex/commit/3d93049d499001fbbb01892534f4045b403f2d04"
        },
        "date": 1723548513477,
        "tool": "cargo",
        "benches": [
          {
            "name": "tpch_q1/vortex-in-memory-no-pushdown",
            "value": 475859933,
            "range": "± 5540414",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-in-memory-pushdown",
            "value": 475012067,
            "range": "± 1934639",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/arrow",
            "value": 462829254,
            "range": "± 3081879",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/parquet",
            "value": 669005458,
            "range": "± 4112973",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-file-compressed",
            "value": 770199687,
            "range": "± 1901607",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-file-uncompressed",
            "value": 639073893,
            "range": "± 2626534",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-in-memory-no-pushdown",
            "value": 145323208,
            "range": "± 513495",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-in-memory-pushdown",
            "value": 173673882,
            "range": "± 1525453",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/arrow",
            "value": 127877742,
            "range": "± 224809",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/parquet",
            "value": 161450992,
            "range": "± 632992",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-file-compressed",
            "value": 164160118,
            "range": "± 609166",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-file-uncompressed",
            "value": 153231704,
            "range": "± 1043132",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-in-memory-no-pushdown",
            "value": 162405363,
            "range": "± 385518",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-in-memory-pushdown",
            "value": 209049026,
            "range": "± 641677",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/arrow",
            "value": 155121790,
            "range": "± 389662",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/parquet",
            "value": 342052926,
            "range": "± 1292909",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-file-compressed",
            "value": 250745453,
            "range": "± 849662",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-file-uncompressed",
            "value": 201170832,
            "range": "± 1532906",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-in-memory-no-pushdown",
            "value": 123195585,
            "range": "± 1275286",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-in-memory-pushdown",
            "value": 124439474,
            "range": "± 519243",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/arrow",
            "value": 119477470,
            "range": "± 634251",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/parquet",
            "value": 230946325,
            "range": "± 2591605",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-file-compressed",
            "value": 242086606,
            "range": "± 1214094",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-file-uncompressed",
            "value": 198834779,
            "range": "± 2412848",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-in-memory-no-pushdown",
            "value": 302585321,
            "range": "± 1424867",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-in-memory-pushdown",
            "value": 304751651,
            "range": "± 1275311",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/arrow",
            "value": 299196052,
            "range": "± 1864918",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/parquet",
            "value": 450545370,
            "range": "± 8277472",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-file-compressed",
            "value": 398476286,
            "range": "± 2869182",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-file-uncompressed",
            "value": 354993554,
            "range": "± 2071073",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-in-memory-no-pushdown",
            "value": 38758625,
            "range": "± 29595",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-in-memory-pushdown",
            "value": 183866021,
            "range": "± 1915668",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/arrow",
            "value": 35444089,
            "range": "± 270749",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/parquet",
            "value": 147873384,
            "range": "± 432554",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-file-compressed",
            "value": 123960523,
            "range": "± 2629400",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-file-uncompressed",
            "value": 99386223,
            "range": "± 1984385",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-in-memory-no-pushdown",
            "value": 566778462,
            "range": "± 1745669",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-in-memory-pushdown",
            "value": 717735123,
            "range": "± 2232843",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/arrow",
            "value": 564625848,
            "range": "± 8593302",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/parquet",
            "value": 725919896,
            "range": "± 7940449",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-file-compressed",
            "value": 698853750,
            "range": "± 9895933",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-file-uncompressed",
            "value": 648209537,
            "range": "± 48841340",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-in-memory-no-pushdown",
            "value": 226083862,
            "range": "± 1719270",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-in-memory-pushdown",
            "value": 1983613862,
            "range": "± 6477045",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/arrow",
            "value": 224835881,
            "range": "± 1583228",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/parquet",
            "value": 487400834,
            "range": "± 3284981",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-file-compressed",
            "value": 321260503,
            "range": "± 3670404",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-file-uncompressed",
            "value": 258337633,
            "range": "± 9674036",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-in-memory-no-pushdown",
            "value": 418979851,
            "range": "± 4811646",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-in-memory-pushdown",
            "value": 420603880,
            "range": "± 6589937",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/arrow",
            "value": 411003397,
            "range": "± 1264261",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/parquet",
            "value": 710238939,
            "range": "± 2914223",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-file-compressed",
            "value": 564309893,
            "range": "± 10359628",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-file-uncompressed",
            "value": 489802716,
            "range": "± 9607891",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-in-memory-no-pushdown",
            "value": 239686481,
            "range": "± 3940730",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-in-memory-pushdown",
            "value": 333912422,
            "range": "± 1053773",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/arrow",
            "value": 234216334,
            "range": "± 735202",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/parquet",
            "value": 484384658,
            "range": "± 4933965",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-file-compressed",
            "value": 485029799,
            "range": "± 1519486",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-file-uncompressed",
            "value": 341975048,
            "range": "± 1364698",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-in-memory-no-pushdown",
            "value": 217658952,
            "range": "± 554392",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-in-memory-pushdown",
            "value": 605491853,
            "range": "± 2032604",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/arrow",
            "value": 178951907,
            "range": "± 589375",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/parquet",
            "value": 190790464,
            "range": "± 621379",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-file-compressed",
            "value": 172777462,
            "range": "± 1428957",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-file-uncompressed",
            "value": 165635875,
            "range": "± 573587",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-in-memory-no-pushdown",
            "value": 182278440,
            "range": "± 629282",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-in-memory-pushdown",
            "value": 224808978,
            "range": "± 463224",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/arrow",
            "value": 173498661,
            "range": "± 1434503",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/parquet",
            "value": 355186279,
            "range": "± 466927",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-file-compressed",
            "value": 433931017,
            "range": "± 1981002",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-file-uncompressed",
            "value": 336355686,
            "range": "± 841892",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-in-memory-no-pushdown",
            "value": 292075118,
            "range": "± 4751826",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-in-memory-pushdown",
            "value": 294002583,
            "range": "± 7724155",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/arrow",
            "value": 289883976,
            "range": "± 9592881",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/parquet",
            "value": 429733529,
            "range": "± 4763466",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-file-compressed",
            "value": 343049223,
            "range": "± 2708347",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-file-uncompressed",
            "value": 342347174,
            "range": "± 2104033",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-in-memory-no-pushdown",
            "value": 38458008,
            "range": "± 151675",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-in-memory-pushdown",
            "value": 38920954,
            "range": "± 100437",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/arrow",
            "value": 37425075,
            "range": "± 74268",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/parquet",
            "value": 216091149,
            "range": "± 470466",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-file-compressed",
            "value": 128507731,
            "range": "± 831916",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-file-uncompressed",
            "value": 106531522,
            "range": "± 6268488",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-in-memory-no-pushdown",
            "value": 123258976,
            "range": "± 1400335",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-in-memory-pushdown",
            "value": 44310912,
            "range": "± 229706",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/arrow",
            "value": 108759673,
            "range": "± 386730",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/parquet",
            "value": 130291828,
            "range": "± 2490501",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-file-compressed",
            "value": 142677144,
            "range": "± 591675",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-file-uncompressed",
            "value": 138125980,
            "range": "± 388340",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-in-memory-no-pushdown",
            "value": 587606746,
            "range": "± 16454189",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-in-memory-pushdown",
            "value": 1136279872,
            "range": "± 5487819",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/arrow",
            "value": 508884786,
            "range": "± 12454955",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/parquet",
            "value": 595139653,
            "range": "± 6377120",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-file-compressed",
            "value": 697235732,
            "range": "± 12118977",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-file-uncompressed",
            "value": 653601715,
            "range": "± 4292681",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-in-memory-no-pushdown",
            "value": 1094289263,
            "range": "± 47817924",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-in-memory-pushdown",
            "value": 1038246469,
            "range": "± 6224095",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/arrow",
            "value": 1032013064,
            "range": "± 7657321",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/parquet",
            "value": 1214889327,
            "range": "± 5848446",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-file-compressed",
            "value": 1189948687,
            "range": "± 3937503",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-file-uncompressed",
            "value": 1160535535,
            "range": "± 17057275",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-in-memory-no-pushdown",
            "value": 171466294,
            "range": "± 261237",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-in-memory-pushdown",
            "value": 504948608,
            "range": "± 2902146",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/arrow",
            "value": 157151636,
            "range": "± 297635",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/parquet",
            "value": 471030423,
            "range": "± 1937324",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-file-compressed",
            "value": 1211130208,
            "range": "± 6580391",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-file-uncompressed",
            "value": 779362657,
            "range": "± 4172819",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-in-memory-no-pushdown",
            "value": 264948077,
            "range": "± 2268531",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-in-memory-pushdown",
            "value": 263294856,
            "range": "± 1588258",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/arrow",
            "value": 243779334,
            "range": "± 4450879",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/parquet",
            "value": 362608023,
            "range": "± 10940360",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-file-compressed",
            "value": 368435647,
            "range": "± 3042466",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-file-uncompressed",
            "value": 356202810,
            "range": "± 13563919",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-in-memory-no-pushdown",
            "value": 914172135,
            "range": "± 5081435",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-in-memory-pushdown",
            "value": 1583228938,
            "range": "± 5405983",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/arrow",
            "value": 899861738,
            "range": "± 3029942",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/parquet",
            "value": 1050204174,
            "range": "± 3984259",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-file-compressed",
            "value": 925772819,
            "range": "± 18554363",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-file-uncompressed",
            "value": 794040749,
            "range": "± 14602762",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-in-memory-no-pushdown",
            "value": 95855375,
            "range": "± 224905",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-in-memory-pushdown",
            "value": 96889740,
            "range": "± 3457319",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/arrow",
            "value": 67421116,
            "range": "± 988582",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/parquet",
            "value": 101274608,
            "range": "± 3250779",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-file-compressed",
            "value": 112915181,
            "range": "± 739421",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-file-uncompressed",
            "value": 112285280,
            "range": "± 475834",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "adam@spiraldb.com",
            "name": "Adam Gutglick",
            "username": "AdamGS"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "f9a6f20b1a0744bc74941727cdfca95e14353f38",
          "message": "Basic fuzzing for compression and slicing functions (#600)\n\nJust basic fuzzing for compressed arrays. Already points at some\r\nissues/bugs in `BitPackedCompressor`, `DictCompressor` and potentially\r\n`ZigZag`.\r\nAlso includes a fix for `scalar_at` for bitpacked arrays (didn't respect\r\nvalidity correctly for non-patched values) and `is_constant` statistics for nullable arrays.",
          "timestamp": "2024-08-13T11:22:15Z",
          "tree_id": "e4098896e6c1c4c0365c7b48d244688931056083",
          "url": "https://github.com/spiraldb/vortex/commit/f9a6f20b1a0744bc74941727cdfca95e14353f38"
        },
        "date": 1723550106005,
        "tool": "cargo",
        "benches": [
          {
            "name": "tpch_q1/vortex-in-memory-no-pushdown",
            "value": 484527960,
            "range": "± 4253270",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-in-memory-pushdown",
            "value": 485661154,
            "range": "± 1231365",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/arrow",
            "value": 471691044,
            "range": "± 1264929",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/parquet",
            "value": 680498274,
            "range": "± 2590966",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-file-compressed",
            "value": 782226148,
            "range": "± 2057013",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-file-uncompressed",
            "value": 648220832,
            "range": "± 1899180",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-in-memory-no-pushdown",
            "value": 145488164,
            "range": "± 348227",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-in-memory-pushdown",
            "value": 172312954,
            "range": "± 1398834",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/arrow",
            "value": 128413478,
            "range": "± 309973",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/parquet",
            "value": 161745169,
            "range": "± 2383282",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-file-compressed",
            "value": 168617730,
            "range": "± 1973422",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-file-uncompressed",
            "value": 157729481,
            "range": "± 1869905",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-in-memory-no-pushdown",
            "value": 161599493,
            "range": "± 6472240",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-in-memory-pushdown",
            "value": 211210236,
            "range": "± 3095944",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/arrow",
            "value": 159281660,
            "range": "± 824945",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/parquet",
            "value": 348295099,
            "range": "± 2572480",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-file-compressed",
            "value": 246929711,
            "range": "± 2394535",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-file-uncompressed",
            "value": 192075938,
            "range": "± 3746527",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-in-memory-no-pushdown",
            "value": 122600893,
            "range": "± 312456",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-in-memory-pushdown",
            "value": 126560411,
            "range": "± 1787694",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/arrow",
            "value": 119578745,
            "range": "± 1557219",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/parquet",
            "value": 228687518,
            "range": "± 431268",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-file-compressed",
            "value": 252131277,
            "range": "± 3693437",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-file-uncompressed",
            "value": 196071427,
            "range": "± 1802669",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-in-memory-no-pushdown",
            "value": 309692153,
            "range": "± 1745414",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-in-memory-pushdown",
            "value": 308397002,
            "range": "± 6152884",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/arrow",
            "value": 303597013,
            "range": "± 1506785",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/parquet",
            "value": 446773871,
            "range": "± 3625080",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-file-compressed",
            "value": 399426209,
            "range": "± 2446309",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-file-uncompressed",
            "value": 358379193,
            "range": "± 4901745",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-in-memory-no-pushdown",
            "value": 38807289,
            "range": "± 466654",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-in-memory-pushdown",
            "value": 191841311,
            "range": "± 132448",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/arrow",
            "value": 35536353,
            "range": "± 40497",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/parquet",
            "value": 147365957,
            "range": "± 2035566",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-file-compressed",
            "value": 130649650,
            "range": "± 2540168",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-file-uncompressed",
            "value": 100067555,
            "range": "± 799601",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-in-memory-no-pushdown",
            "value": 568366781,
            "range": "± 12560737",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-in-memory-pushdown",
            "value": 711478800,
            "range": "± 1756234",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/arrow",
            "value": 561778316,
            "range": "± 2638177",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/parquet",
            "value": 719242248,
            "range": "± 4887895",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-file-compressed",
            "value": 690371549,
            "range": "± 6126729",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-file-uncompressed",
            "value": 653486370,
            "range": "± 4350569",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-in-memory-no-pushdown",
            "value": 227666502,
            "range": "± 2380619",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-in-memory-pushdown",
            "value": 2033563499,
            "range": "± 45663480",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/arrow",
            "value": 227379823,
            "range": "± 1489867",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/parquet",
            "value": 496803633,
            "range": "± 4153293",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-file-compressed",
            "value": 321992681,
            "range": "± 5983287",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-file-uncompressed",
            "value": 258263817,
            "range": "± 5596331",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-in-memory-no-pushdown",
            "value": 418805427,
            "range": "± 1252470",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-in-memory-pushdown",
            "value": 419383372,
            "range": "± 8869000",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/arrow",
            "value": 422013342,
            "range": "± 5875312",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/parquet",
            "value": 705026185,
            "range": "± 4232275",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-file-compressed",
            "value": 573271036,
            "range": "± 12513293",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-file-uncompressed",
            "value": 485123178,
            "range": "± 8183548",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-in-memory-no-pushdown",
            "value": 243057691,
            "range": "± 4601406",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-in-memory-pushdown",
            "value": 339853616,
            "range": "± 2227199",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/arrow",
            "value": 235016221,
            "range": "± 4230478",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/parquet",
            "value": 493814661,
            "range": "± 4964668",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-file-compressed",
            "value": 486656677,
            "range": "± 1792574",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-file-uncompressed",
            "value": 350833855,
            "range": "± 2462104",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-in-memory-no-pushdown",
            "value": 234191852,
            "range": "± 7795396",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-in-memory-pushdown",
            "value": 608303403,
            "range": "± 17163182",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/arrow",
            "value": 186080954,
            "range": "± 1563564",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/parquet",
            "value": 198242204,
            "range": "± 583634",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-file-compressed",
            "value": 178542136,
            "range": "± 1761487",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-file-uncompressed",
            "value": 173882227,
            "range": "± 904262",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-in-memory-no-pushdown",
            "value": 183791469,
            "range": "± 205895",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-in-memory-pushdown",
            "value": 225510594,
            "range": "± 393908",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/arrow",
            "value": 174336270,
            "range": "± 2110695",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/parquet",
            "value": 358735324,
            "range": "± 1123335",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-file-compressed",
            "value": 447267039,
            "range": "± 7234283",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-file-uncompressed",
            "value": 341162741,
            "range": "± 2623875",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-in-memory-no-pushdown",
            "value": 309855672,
            "range": "± 3842374",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-in-memory-pushdown",
            "value": 316581737,
            "range": "± 6213855",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/arrow",
            "value": 329192932,
            "range": "± 7778224",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/parquet",
            "value": 456645095,
            "range": "± 4821198",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-file-compressed",
            "value": 355803972,
            "range": "± 3740814",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-file-uncompressed",
            "value": 367206205,
            "range": "± 8882434",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-in-memory-no-pushdown",
            "value": 40635660,
            "range": "± 1201766",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-in-memory-pushdown",
            "value": 40478344,
            "range": "± 1678569",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/arrow",
            "value": 40143211,
            "range": "± 637923",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/parquet",
            "value": 220122425,
            "range": "± 1784109",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-file-compressed",
            "value": 131007221,
            "range": "± 2094366",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-file-uncompressed",
            "value": 107108824,
            "range": "± 1840873",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-in-memory-no-pushdown",
            "value": 123148756,
            "range": "± 897182",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-in-memory-pushdown",
            "value": 44415872,
            "range": "± 433419",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/arrow",
            "value": 110055714,
            "range": "± 1231322",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/parquet",
            "value": 126245561,
            "range": "± 601092",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-file-compressed",
            "value": 145455042,
            "range": "± 876358",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-file-uncompressed",
            "value": 139149976,
            "range": "± 753512",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-in-memory-no-pushdown",
            "value": 609719690,
            "range": "± 16494340",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-in-memory-pushdown",
            "value": 1219950982,
            "range": "± 38819565",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/arrow",
            "value": 539598840,
            "range": "± 15008842",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/parquet",
            "value": 592519165,
            "range": "± 14227740",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-file-compressed",
            "value": 696016624,
            "range": "± 5382282",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-file-uncompressed",
            "value": 655766110,
            "range": "± 5745575",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-in-memory-no-pushdown",
            "value": 1067606595,
            "range": "± 49849912",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-in-memory-pushdown",
            "value": 1067279216,
            "range": "± 14774635",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/arrow",
            "value": 1066520081,
            "range": "± 20638633",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/parquet",
            "value": 1232636366,
            "range": "± 7783092",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-file-compressed",
            "value": 1230719631,
            "range": "± 10947974",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-file-uncompressed",
            "value": 1145454188,
            "range": "± 9162414",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-in-memory-no-pushdown",
            "value": 172012769,
            "range": "± 1338433",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-in-memory-pushdown",
            "value": 533607550,
            "range": "± 734056",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/arrow",
            "value": 157690119,
            "range": "± 2018104",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/parquet",
            "value": 477220475,
            "range": "± 1990191",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-file-compressed",
            "value": 1239345508,
            "range": "± 12332343",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-file-uncompressed",
            "value": 784060705,
            "range": "± 2731247",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-in-memory-no-pushdown",
            "value": 266249192,
            "range": "± 2520470",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-in-memory-pushdown",
            "value": 265448632,
            "range": "± 1838136",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/arrow",
            "value": 250273953,
            "range": "± 2794599",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/parquet",
            "value": 364320368,
            "range": "± 5634236",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-file-compressed",
            "value": 375590117,
            "range": "± 5411564",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-file-uncompressed",
            "value": 359930331,
            "range": "± 11298655",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-in-memory-no-pushdown",
            "value": 911549073,
            "range": "± 6987434",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-in-memory-pushdown",
            "value": 1609515453,
            "range": "± 23539514",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/arrow",
            "value": 902812188,
            "range": "± 4976448",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/parquet",
            "value": 1056171337,
            "range": "± 11864824",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-file-compressed",
            "value": 913368944,
            "range": "± 11274335",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-file-uncompressed",
            "value": 793177630,
            "range": "± 12211185",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-in-memory-no-pushdown",
            "value": 96131298,
            "range": "± 436460",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-in-memory-pushdown",
            "value": 98267627,
            "range": "± 2036492",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/arrow",
            "value": 66448211,
            "range": "± 1210260",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/parquet",
            "value": 94666295,
            "range": "± 202098",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-file-compressed",
            "value": 115505728,
            "range": "± 789317",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-file-uncompressed",
            "value": 114836459,
            "range": "± 552678",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "adam@spiraldb.com",
            "name": "Adam Gutglick",
            "username": "AdamGS"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "fc138b705ae8a176729ff9f814f89519e1bcc746",
          "message": "Add nightly fuzzing job (#612)",
          "timestamp": "2024-08-13T13:54:20Z",
          "tree_id": "040b3da5db91ce3c3dba337bec8b0b525b9794ee",
          "url": "https://github.com/spiraldb/vortex/commit/fc138b705ae8a176729ff9f814f89519e1bcc746"
        },
        "date": 1723559540059,
        "tool": "cargo",
        "benches": [
          {
            "name": "tpch_q1/vortex-in-memory-no-pushdown",
            "value": 498330013,
            "range": "± 6919300",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-in-memory-pushdown",
            "value": 494359159,
            "range": "± 5268575",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/arrow",
            "value": 494495475,
            "range": "± 5519188",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/parquet",
            "value": 699673782,
            "range": "± 3596257",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-file-compressed",
            "value": 845361212,
            "range": "± 5746421",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-file-uncompressed",
            "value": 694233337,
            "range": "± 5315604",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-in-memory-no-pushdown",
            "value": 168401723,
            "range": "± 3340887",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-in-memory-pushdown",
            "value": 193509126,
            "range": "± 2883006",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/arrow",
            "value": 135595688,
            "range": "± 2329205",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/parquet",
            "value": 180645816,
            "range": "± 1813567",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-file-compressed",
            "value": 195869173,
            "range": "± 4748376",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-file-uncompressed",
            "value": 181491234,
            "range": "± 5905834",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-in-memory-no-pushdown",
            "value": 178981665,
            "range": "± 4742929",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-in-memory-pushdown",
            "value": 244271392,
            "range": "± 9755787",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/arrow",
            "value": 182433130,
            "range": "± 7544875",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/parquet",
            "value": 365028350,
            "range": "± 2101010",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-file-compressed",
            "value": 257184449,
            "range": "± 4958496",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-file-uncompressed",
            "value": 208005019,
            "range": "± 2451677",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-in-memory-no-pushdown",
            "value": 133385930,
            "range": "± 1371003",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-in-memory-pushdown",
            "value": 136182547,
            "range": "± 1444568",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/arrow",
            "value": 132865855,
            "range": "± 2962420",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/parquet",
            "value": 243167274,
            "range": "± 8941313",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-file-compressed",
            "value": 261336541,
            "range": "± 2612565",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-file-uncompressed",
            "value": 221229318,
            "range": "± 2696492",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-in-memory-no-pushdown",
            "value": 362891520,
            "range": "± 13631344",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-in-memory-pushdown",
            "value": 355590932,
            "range": "± 13279515",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/arrow",
            "value": 346144937,
            "range": "± 6229842",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/parquet",
            "value": 495158690,
            "range": "± 4409474",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-file-compressed",
            "value": 437826000,
            "range": "± 9180751",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-file-uncompressed",
            "value": 405328686,
            "range": "± 12900792",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-in-memory-no-pushdown",
            "value": 42186126,
            "range": "± 311792",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-in-memory-pushdown",
            "value": 195461431,
            "range": "± 1103922",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/arrow",
            "value": 39162765,
            "range": "± 409054",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/parquet",
            "value": 155816361,
            "range": "± 1709465",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-file-compressed",
            "value": 130801512,
            "range": "± 743405",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-file-uncompressed",
            "value": 106658322,
            "range": "± 1340956",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-in-memory-no-pushdown",
            "value": 666734874,
            "range": "± 20694629",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-in-memory-pushdown",
            "value": 862282546,
            "range": "± 29033258",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/arrow",
            "value": 721705249,
            "range": "± 6005169",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/parquet",
            "value": 890704934,
            "range": "± 18561556",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-file-compressed",
            "value": 899088847,
            "range": "± 17035256",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-file-uncompressed",
            "value": 829977480,
            "range": "± 10179635",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-in-memory-no-pushdown",
            "value": 250585584,
            "range": "± 8615651",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-in-memory-pushdown",
            "value": 2576501063,
            "range": "± 144860696",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/arrow",
            "value": 241242081,
            "range": "± 3166599",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/parquet",
            "value": 517037630,
            "range": "± 8768848",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-file-compressed",
            "value": 333912371,
            "range": "± 2789109",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-file-uncompressed",
            "value": 277764053,
            "range": "± 4533763",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-in-memory-no-pushdown",
            "value": 459118153,
            "range": "± 6128954",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-in-memory-pushdown",
            "value": 455253054,
            "range": "± 6982383",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/arrow",
            "value": 448265842,
            "range": "± 4147582",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/parquet",
            "value": 748541811,
            "range": "± 9644656",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-file-compressed",
            "value": 599005370,
            "range": "± 9121282",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-file-uncompressed",
            "value": 557949155,
            "range": "± 14069037",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-in-memory-no-pushdown",
            "value": 260518703,
            "range": "± 2426350",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-in-memory-pushdown",
            "value": 354979036,
            "range": "± 2641448",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/arrow",
            "value": 250604493,
            "range": "± 1250945",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/parquet",
            "value": 510301736,
            "range": "± 9943945",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-file-compressed",
            "value": 508624388,
            "range": "± 4972292",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-file-uncompressed",
            "value": 354181779,
            "range": "± 2062029",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-in-memory-no-pushdown",
            "value": 254027316,
            "range": "± 4067705",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-in-memory-pushdown",
            "value": 654444592,
            "range": "± 4186312",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/arrow",
            "value": 192851661,
            "range": "± 1114846",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/parquet",
            "value": 206115504,
            "range": "± 1276226",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-file-compressed",
            "value": 197250485,
            "range": "± 4866493",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-file-uncompressed",
            "value": 184172744,
            "range": "± 9102842",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-in-memory-no-pushdown",
            "value": 182847120,
            "range": "± 692379",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-in-memory-pushdown",
            "value": 228139707,
            "range": "± 769640",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/arrow",
            "value": 172796896,
            "range": "± 2186158",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/parquet",
            "value": 358140700,
            "range": "± 3546710",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-file-compressed",
            "value": 442873163,
            "range": "± 5117242",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-file-uncompressed",
            "value": 342874601,
            "range": "± 2396596",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-in-memory-no-pushdown",
            "value": 343024341,
            "range": "± 9281350",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-in-memory-pushdown",
            "value": 355268671,
            "range": "± 12741175",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/arrow",
            "value": 335371438,
            "range": "± 5322651",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/parquet",
            "value": 481882391,
            "range": "± 12039598",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-file-compressed",
            "value": 374786927,
            "range": "± 5559531",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-file-uncompressed",
            "value": 358248919,
            "range": "± 8905451",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-in-memory-no-pushdown",
            "value": 42891739,
            "range": "± 1104152",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-in-memory-pushdown",
            "value": 43224614,
            "range": "± 345494",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/arrow",
            "value": 44965370,
            "range": "± 465672",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/parquet",
            "value": 230928790,
            "range": "± 2337318",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-file-compressed",
            "value": 134866865,
            "range": "± 2061233",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-file-uncompressed",
            "value": 110858170,
            "range": "± 827344",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-in-memory-no-pushdown",
            "value": 131809818,
            "range": "± 1188830",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-in-memory-pushdown",
            "value": 46973677,
            "range": "± 436184",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/arrow",
            "value": 114542927,
            "range": "± 1236093",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/parquet",
            "value": 132870566,
            "range": "± 1704805",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-file-compressed",
            "value": 152321601,
            "range": "± 755350",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-file-uncompressed",
            "value": 145326733,
            "range": "± 1199414",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-in-memory-no-pushdown",
            "value": 687869866,
            "range": "± 15506341",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-in-memory-pushdown",
            "value": 1356419082,
            "range": "± 32361499",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/arrow",
            "value": 612806470,
            "range": "± 24326341",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/parquet",
            "value": 628282575,
            "range": "± 13923445",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-file-compressed",
            "value": 754117464,
            "range": "± 19626162",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-file-uncompressed",
            "value": 715991929,
            "range": "± 16202952",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-in-memory-no-pushdown",
            "value": 1352010832,
            "range": "± 21923046",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-in-memory-pushdown",
            "value": 1299044125,
            "range": "± 25297604",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/arrow",
            "value": 1216685598,
            "range": "± 29107674",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/parquet",
            "value": 1416501753,
            "range": "± 44746860",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-file-compressed",
            "value": 1377899770,
            "range": "± 28313789",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-file-uncompressed",
            "value": 1333827469,
            "range": "± 39963848",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-in-memory-no-pushdown",
            "value": 176987173,
            "range": "± 2774524",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-in-memory-pushdown",
            "value": 554389247,
            "range": "± 4779494",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/arrow",
            "value": 161408808,
            "range": "± 739471",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/parquet",
            "value": 492941471,
            "range": "± 2196075",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-file-compressed",
            "value": 1319371900,
            "range": "± 21646259",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-file-uncompressed",
            "value": 802080514,
            "range": "± 8004470",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-in-memory-no-pushdown",
            "value": 327378364,
            "range": "± 3971975",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-in-memory-pushdown",
            "value": 330675036,
            "range": "± 7669037",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/arrow",
            "value": 279049679,
            "range": "± 7925300",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/parquet",
            "value": 397130094,
            "range": "± 6108063",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-file-compressed",
            "value": 399664468,
            "range": "± 4756585",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-file-uncompressed",
            "value": 363921739,
            "range": "± 10852849",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-in-memory-no-pushdown",
            "value": 982918398,
            "range": "± 8558157",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-in-memory-pushdown",
            "value": 1727716371,
            "range": "± 19111807",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/arrow",
            "value": 943879061,
            "range": "± 18998117",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/parquet",
            "value": 1087922471,
            "range": "± 8166827",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-file-compressed",
            "value": 953556467,
            "range": "± 13211539",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-file-uncompressed",
            "value": 812815737,
            "range": "± 6211748",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-in-memory-no-pushdown",
            "value": 96625716,
            "range": "± 299960",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-in-memory-pushdown",
            "value": 97760523,
            "range": "± 279104",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/arrow",
            "value": 67141692,
            "range": "± 999203",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/parquet",
            "value": 96014534,
            "range": "± 643873",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-file-compressed",
            "value": 113109557,
            "range": "± 1223790",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-file-uncompressed",
            "value": 112591646,
            "range": "± 522906",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "adam@spiraldb.com",
            "name": "Adam Gutglick",
            "username": "AdamGS"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "fdc025a7e475eb76e6879ccf763acd692d5ede50",
          "message": "Get beyond the immediate fuzzing failures (#611)\n\nOvercome some float-related issues to keep the fuzzer running",
          "timestamp": "2024-08-13T13:55:10Z",
          "tree_id": "26c632a31b0e6ba6efc00cfc5e1d49f1cbf885a7",
          "url": "https://github.com/spiraldb/vortex/commit/fdc025a7e475eb76e6879ccf763acd692d5ede50"
        },
        "date": 1723560222927,
        "tool": "cargo",
        "benches": [
          {
            "name": "tpch_q1/vortex-in-memory-no-pushdown",
            "value": 485404333,
            "range": "± 1425152",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-in-memory-pushdown",
            "value": 486457684,
            "range": "± 1426257",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/arrow",
            "value": 475296643,
            "range": "± 2239904",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/parquet",
            "value": 676886299,
            "range": "± 1646047",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-file-compressed",
            "value": 779483839,
            "range": "± 4425468",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-file-uncompressed",
            "value": 642286827,
            "range": "± 2706878",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-in-memory-no-pushdown",
            "value": 150798216,
            "range": "± 900295",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-in-memory-pushdown",
            "value": 178810334,
            "range": "± 525811",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/arrow",
            "value": 129690724,
            "range": "± 1620643",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/parquet",
            "value": 165253852,
            "range": "± 481111",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-file-compressed",
            "value": 170460253,
            "range": "± 1276631",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-file-uncompressed",
            "value": 162457286,
            "range": "± 848584",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-in-memory-no-pushdown",
            "value": 165996320,
            "range": "± 1300164",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-in-memory-pushdown",
            "value": 220019140,
            "range": "± 4906081",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/arrow",
            "value": 161704063,
            "range": "± 609149",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/parquet",
            "value": 350469235,
            "range": "± 1970549",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-file-compressed",
            "value": 251587677,
            "range": "± 4356595",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-file-uncompressed",
            "value": 200160961,
            "range": "± 2017166",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-in-memory-no-pushdown",
            "value": 127691543,
            "range": "± 918019",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-in-memory-pushdown",
            "value": 128934851,
            "range": "± 542656",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/arrow",
            "value": 123571228,
            "range": "± 1867818",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/parquet",
            "value": 233769679,
            "range": "± 947168",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-file-compressed",
            "value": 251560113,
            "range": "± 2735199",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-file-uncompressed",
            "value": 198870670,
            "range": "± 2796400",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-in-memory-no-pushdown",
            "value": 313372180,
            "range": "± 1420846",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-in-memory-pushdown",
            "value": 314733871,
            "range": "± 2366090",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/arrow",
            "value": 311906788,
            "range": "± 4765112",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/parquet",
            "value": 468373526,
            "range": "± 2990482",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-file-compressed",
            "value": 410041153,
            "range": "± 2931119",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-file-uncompressed",
            "value": 366391817,
            "range": "± 4297206",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-in-memory-no-pushdown",
            "value": 40891116,
            "range": "± 344709",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-in-memory-pushdown",
            "value": 195425355,
            "range": "± 1576016",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/arrow",
            "value": 37114171,
            "range": "± 234993",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/parquet",
            "value": 148551291,
            "range": "± 1398628",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-file-compressed",
            "value": 126744485,
            "range": "± 605287",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-file-uncompressed",
            "value": 98785733,
            "range": "± 528011",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-in-memory-no-pushdown",
            "value": 597147814,
            "range": "± 11407947",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-in-memory-pushdown",
            "value": 754580765,
            "range": "± 4106191",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/arrow",
            "value": 588078237,
            "range": "± 2966668",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/parquet",
            "value": 748211538,
            "range": "± 7163331",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-file-compressed",
            "value": 720858766,
            "range": "± 7329199",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-file-uncompressed",
            "value": 673785663,
            "range": "± 8700433",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-in-memory-no-pushdown",
            "value": 236570084,
            "range": "± 1401182",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-in-memory-pushdown",
            "value": 2115946283,
            "range": "± 15164464",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/arrow",
            "value": 231181331,
            "range": "± 2489752",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/parquet",
            "value": 493059476,
            "range": "± 2425961",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-file-compressed",
            "value": 326624723,
            "range": "± 2232993",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-file-uncompressed",
            "value": 263379455,
            "range": "± 3931177",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-in-memory-no-pushdown",
            "value": 439526073,
            "range": "± 4401677",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-in-memory-pushdown",
            "value": 439327020,
            "range": "± 5540969",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/arrow",
            "value": 421435718,
            "range": "± 2919033",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/parquet",
            "value": 717452092,
            "range": "± 7871780",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-file-compressed",
            "value": 555637199,
            "range": "± 6084387",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-file-uncompressed",
            "value": 500815170,
            "range": "± 9891075",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-in-memory-no-pushdown",
            "value": 246973921,
            "range": "± 1581121",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-in-memory-pushdown",
            "value": 343697626,
            "range": "± 2358383",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/arrow",
            "value": 238245361,
            "range": "± 588515",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/parquet",
            "value": 488497743,
            "range": "± 2652159",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-file-compressed",
            "value": 479043432,
            "range": "± 1517708",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-file-uncompressed",
            "value": 340953793,
            "range": "± 1465549",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-in-memory-no-pushdown",
            "value": 228959341,
            "range": "± 3814954",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-in-memory-pushdown",
            "value": 617002082,
            "range": "± 4015172",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/arrow",
            "value": 183739930,
            "range": "± 747835",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/parquet",
            "value": 198005200,
            "range": "± 4903327",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-file-compressed",
            "value": 179131372,
            "range": "± 1447007",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-file-uncompressed",
            "value": 177974997,
            "range": "± 2022804",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-in-memory-no-pushdown",
            "value": 183674127,
            "range": "± 266671",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-in-memory-pushdown",
            "value": 232641563,
            "range": "± 548268",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/arrow",
            "value": 174055177,
            "range": "± 818274",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/parquet",
            "value": 359876524,
            "range": "± 2100066",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-file-compressed",
            "value": 437993964,
            "range": "± 1904716",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-file-uncompressed",
            "value": 334479463,
            "range": "± 4704483",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-in-memory-no-pushdown",
            "value": 325757566,
            "range": "± 6955851",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-in-memory-pushdown",
            "value": 314945730,
            "range": "± 2821328",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/arrow",
            "value": 312726483,
            "range": "± 5379240",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/parquet",
            "value": 465734579,
            "range": "± 2793363",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-file-compressed",
            "value": 356945611,
            "range": "± 2641874",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-file-uncompressed",
            "value": 356010057,
            "range": "± 4541555",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-in-memory-no-pushdown",
            "value": 40701115,
            "range": "± 336612",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-in-memory-pushdown",
            "value": 41400418,
            "range": "± 171331",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/arrow",
            "value": 41832965,
            "range": "± 319568",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/parquet",
            "value": 221633948,
            "range": "± 977658",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-file-compressed",
            "value": 134171634,
            "range": "± 1176588",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-file-uncompressed",
            "value": 107054483,
            "range": "± 788234",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-in-memory-no-pushdown",
            "value": 126429583,
            "range": "± 703571",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-in-memory-pushdown",
            "value": 47492935,
            "range": "± 421061",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/arrow",
            "value": 110837015,
            "range": "± 305500",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/parquet",
            "value": 129023464,
            "range": "± 424527",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-file-compressed",
            "value": 147411746,
            "range": "± 982363",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-file-uncompressed",
            "value": 141861702,
            "range": "± 856669",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-in-memory-no-pushdown",
            "value": 631901877,
            "range": "± 11719320",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-in-memory-pushdown",
            "value": 1255479419,
            "range": "± 13310920",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/arrow",
            "value": 545719274,
            "range": "± 7787823",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/parquet",
            "value": 598550981,
            "range": "± 2779661",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-file-compressed",
            "value": 713201774,
            "range": "± 4550233",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-file-uncompressed",
            "value": 665249240,
            "range": "± 6435604",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-in-memory-no-pushdown",
            "value": 1126984897,
            "range": "± 10004345",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-in-memory-pushdown",
            "value": 1118942291,
            "range": "± 13610821",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/arrow",
            "value": 1112088269,
            "range": "± 16942641",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/parquet",
            "value": 1301622110,
            "range": "± 12079239",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-file-compressed",
            "value": 1271439617,
            "range": "± 19409815",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-file-uncompressed",
            "value": 1204224445,
            "range": "± 14260882",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-in-memory-no-pushdown",
            "value": 171715231,
            "range": "± 510099",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-in-memory-pushdown",
            "value": 543506204,
            "range": "± 5327614",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/arrow",
            "value": 158271056,
            "range": "± 351819",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/parquet",
            "value": 471370751,
            "range": "± 732871",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-file-compressed",
            "value": 1225445852,
            "range": "± 3308521",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-file-uncompressed",
            "value": 777278195,
            "range": "± 1744983",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-in-memory-no-pushdown",
            "value": 280467896,
            "range": "± 5188085",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-in-memory-pushdown",
            "value": 290664766,
            "range": "± 4777126",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/arrow",
            "value": 267732628,
            "range": "± 2940549",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/parquet",
            "value": 373035323,
            "range": "± 2499151",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-file-compressed",
            "value": 390870070,
            "range": "± 4381642",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-file-uncompressed",
            "value": 368848862,
            "range": "± 2515854",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-in-memory-no-pushdown",
            "value": 953605960,
            "range": "± 4784194",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-in-memory-pushdown",
            "value": 1649946904,
            "range": "± 10874835",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/arrow",
            "value": 937215103,
            "range": "± 7023260",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/parquet",
            "value": 1106730123,
            "range": "± 7206077",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-file-compressed",
            "value": 931177730,
            "range": "± 15653263",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-file-uncompressed",
            "value": 814436382,
            "range": "± 9378049",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-in-memory-no-pushdown",
            "value": 98653608,
            "range": "± 550443",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-in-memory-pushdown",
            "value": 98902198,
            "range": "± 306012",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/arrow",
            "value": 68616584,
            "range": "± 416169",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/parquet",
            "value": 96936450,
            "range": "± 549960",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-file-compressed",
            "value": 116227901,
            "range": "± 1161702",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-file-uncompressed",
            "value": 114889123,
            "range": "± 483003",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "adam@spiraldb.com",
            "name": "Adam Gutglick",
            "username": "AdamGS"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "e7e97a578fa8e34fbfc907ebbba233f44e9651d9",
          "message": "Fix the fuzzing GH action (#613)",
          "timestamp": "2024-08-13T14:13:20Z",
          "tree_id": "d779c010683f1e09f2eef07a4a9ccd6f26d087a2",
          "url": "https://github.com/spiraldb/vortex/commit/e7e97a578fa8e34fbfc907ebbba233f44e9651d9"
        },
        "date": 1723561708625,
        "tool": "cargo",
        "benches": [
          {
            "name": "tpch_q1/vortex-in-memory-no-pushdown",
            "value": 488861321,
            "range": "± 8110861",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-in-memory-pushdown",
            "value": 492170662,
            "range": "± 7382423",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/arrow",
            "value": 474079223,
            "range": "± 1167720",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/parquet",
            "value": 678235486,
            "range": "± 2885164",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-file-compressed",
            "value": 784786121,
            "range": "± 5369388",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-file-uncompressed",
            "value": 658013129,
            "range": "± 5041941",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-in-memory-no-pushdown",
            "value": 155522792,
            "range": "± 1496351",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-in-memory-pushdown",
            "value": 180772768,
            "range": "± 2027468",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/arrow",
            "value": 132544725,
            "range": "± 1705737",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/parquet",
            "value": 170889353,
            "range": "± 2779756",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-file-compressed",
            "value": 182695204,
            "range": "± 2672857",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-file-uncompressed",
            "value": 161666552,
            "range": "± 2367990",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-in-memory-no-pushdown",
            "value": 166946194,
            "range": "± 3465282",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-in-memory-pushdown",
            "value": 242570162,
            "range": "± 5391715",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/arrow",
            "value": 163551854,
            "range": "± 2031416",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/parquet",
            "value": 349635142,
            "range": "± 3259135",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-file-compressed",
            "value": 258886320,
            "range": "± 3192900",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-file-uncompressed",
            "value": 203208080,
            "range": "± 2155621",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-in-memory-no-pushdown",
            "value": 127973518,
            "range": "± 1747201",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-in-memory-pushdown",
            "value": 136045466,
            "range": "± 2193823",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/arrow",
            "value": 125214370,
            "range": "± 1455421",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/parquet",
            "value": 244419978,
            "range": "± 3663581",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-file-compressed",
            "value": 254273459,
            "range": "± 4583591",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-file-uncompressed",
            "value": 208270033,
            "range": "± 3480339",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-in-memory-no-pushdown",
            "value": 320865716,
            "range": "± 4652565",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-in-memory-pushdown",
            "value": 318525156,
            "range": "± 3123383",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/arrow",
            "value": 321064629,
            "range": "± 5162545",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/parquet",
            "value": 483975726,
            "range": "± 4269221",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-file-compressed",
            "value": 415299380,
            "range": "± 6616547",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-file-uncompressed",
            "value": 368750827,
            "range": "± 8171255",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-in-memory-no-pushdown",
            "value": 42209455,
            "range": "± 647183",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-in-memory-pushdown",
            "value": 189529785,
            "range": "± 1827679",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/arrow",
            "value": 36506525,
            "range": "± 433675",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/parquet",
            "value": 148457688,
            "range": "± 1146481",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-file-compressed",
            "value": 125140148,
            "range": "± 1006702",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-file-uncompressed",
            "value": 97589248,
            "range": "± 1085466",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-in-memory-no-pushdown",
            "value": 569505709,
            "range": "± 3688393",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-in-memory-pushdown",
            "value": 756943238,
            "range": "± 7322484",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/arrow",
            "value": 573903756,
            "range": "± 1767227",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/parquet",
            "value": 731347251,
            "range": "± 4957471",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-file-compressed",
            "value": 702755798,
            "range": "± 4771194",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-file-uncompressed",
            "value": 648449694,
            "range": "± 4208730",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-in-memory-no-pushdown",
            "value": 225271943,
            "range": "± 1301644",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-in-memory-pushdown",
            "value": 2108934463,
            "range": "± 54589569",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/arrow",
            "value": 226166430,
            "range": "± 2508791",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/parquet",
            "value": 493069087,
            "range": "± 4594151",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-file-compressed",
            "value": 334987276,
            "range": "± 4242679",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-file-uncompressed",
            "value": 269604312,
            "range": "± 2985975",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-in-memory-no-pushdown",
            "value": 437512537,
            "range": "± 9761762",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-in-memory-pushdown",
            "value": 440530856,
            "range": "± 8257218",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/arrow",
            "value": 436825606,
            "range": "± 10077958",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/parquet",
            "value": 739295911,
            "range": "± 10354992",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-file-compressed",
            "value": 567216869,
            "range": "± 11334072",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-file-uncompressed",
            "value": 532543211,
            "range": "± 14785158",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-in-memory-no-pushdown",
            "value": 253383799,
            "range": "± 1825501",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-in-memory-pushdown",
            "value": 348037153,
            "range": "± 3853665",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/arrow",
            "value": 248248483,
            "range": "± 2443790",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/parquet",
            "value": 505710952,
            "range": "± 2941706",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-file-compressed",
            "value": 487482705,
            "range": "± 2997765",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-file-uncompressed",
            "value": 346058289,
            "range": "± 2937475",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-in-memory-no-pushdown",
            "value": 234147637,
            "range": "± 1768756",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-in-memory-pushdown",
            "value": 649267650,
            "range": "± 2988222",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/arrow",
            "value": 187419644,
            "range": "± 1391978",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/parquet",
            "value": 205333998,
            "range": "± 2728045",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-file-compressed",
            "value": 183469467,
            "range": "± 3084061",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-file-uncompressed",
            "value": 184951292,
            "range": "± 4870360",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-in-memory-no-pushdown",
            "value": 186125340,
            "range": "± 1035343",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-in-memory-pushdown",
            "value": 229582015,
            "range": "± 3328408",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/arrow",
            "value": 174014675,
            "range": "± 1168054",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/parquet",
            "value": 365124559,
            "range": "± 4031403",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-file-compressed",
            "value": 437641415,
            "range": "± 5332968",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-file-uncompressed",
            "value": 343997023,
            "range": "± 1307873",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-in-memory-no-pushdown",
            "value": 361225573,
            "range": "± 2160666",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-in-memory-pushdown",
            "value": 356740079,
            "range": "± 2374888",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/arrow",
            "value": 357405810,
            "range": "± 2259054",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/parquet",
            "value": 499432948,
            "range": "± 5867326",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-file-compressed",
            "value": 377366617,
            "range": "± 2091720",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-file-uncompressed",
            "value": 370080700,
            "range": "± 1833988",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-in-memory-no-pushdown",
            "value": 44480189,
            "range": "± 621469",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-in-memory-pushdown",
            "value": 44607927,
            "range": "± 829790",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/arrow",
            "value": 41576942,
            "range": "± 541293",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/parquet",
            "value": 225480504,
            "range": "± 1698655",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-file-compressed",
            "value": 130866366,
            "range": "± 3352376",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-file-uncompressed",
            "value": 106969744,
            "range": "± 4111493",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-in-memory-no-pushdown",
            "value": 129497147,
            "range": "± 568822",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-in-memory-pushdown",
            "value": 46388040,
            "range": "± 321869",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/arrow",
            "value": 112965659,
            "range": "± 1369264",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/parquet",
            "value": 132594474,
            "range": "± 1236175",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-file-compressed",
            "value": 143968736,
            "range": "± 772928",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-file-uncompressed",
            "value": 142858882,
            "range": "± 1086513",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-in-memory-no-pushdown",
            "value": 649284333,
            "range": "± 20893926",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-in-memory-pushdown",
            "value": 1220167539,
            "range": "± 28567967",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/arrow",
            "value": 539695664,
            "range": "± 12687154",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/parquet",
            "value": 598885016,
            "range": "± 5731112",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-file-compressed",
            "value": 700920201,
            "range": "± 3818525",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-file-uncompressed",
            "value": 661159599,
            "range": "± 2651690",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-in-memory-no-pushdown",
            "value": 1096751257,
            "range": "± 23354894",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-in-memory-pushdown",
            "value": 1093764889,
            "range": "± 14073242",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/arrow",
            "value": 1067081309,
            "range": "± 18886112",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/parquet",
            "value": 1268104718,
            "range": "± 23507627",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-file-compressed",
            "value": 1239648063,
            "range": "± 13563604",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-file-uncompressed",
            "value": 1167919709,
            "range": "± 13203720",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-in-memory-no-pushdown",
            "value": 170023211,
            "range": "± 248659",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-in-memory-pushdown",
            "value": 533248938,
            "range": "± 2369019",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/arrow",
            "value": 155068544,
            "range": "± 877624",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/parquet",
            "value": 472956957,
            "range": "± 2671125",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-file-compressed",
            "value": 1209987438,
            "range": "± 10920034",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-file-uncompressed",
            "value": 779486784,
            "range": "± 11108831",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-in-memory-no-pushdown",
            "value": 266763512,
            "range": "± 3065100",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-in-memory-pushdown",
            "value": 264814982,
            "range": "± 1539570",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/arrow",
            "value": 256243661,
            "range": "± 8794836",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/parquet",
            "value": 363746227,
            "range": "± 6556151",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-file-compressed",
            "value": 370140093,
            "range": "± 3178996",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-file-uncompressed",
            "value": 351743588,
            "range": "± 2978565",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-in-memory-no-pushdown",
            "value": 920599543,
            "range": "± 11078172",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-in-memory-pushdown",
            "value": 1631269403,
            "range": "± 6785662",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/arrow",
            "value": 921841574,
            "range": "± 4150825",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/parquet",
            "value": 1062498348,
            "range": "± 4032253",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-file-compressed",
            "value": 915668777,
            "range": "± 7908976",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-file-uncompressed",
            "value": 786392280,
            "range": "± 11238820",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-in-memory-no-pushdown",
            "value": 97052035,
            "range": "± 1048333",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-in-memory-pushdown",
            "value": 98679051,
            "range": "± 1153065",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/arrow",
            "value": 66733556,
            "range": "± 575050",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/parquet",
            "value": 97887188,
            "range": "± 322607",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-file-compressed",
            "value": 112686994,
            "range": "± 966339",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-file-uncompressed",
            "value": 111775560,
            "range": "± 554739",
            "unit": "ns/iter"
          }
        ]
      },
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
          "id": "7267db497864f96655a631a34f1a36201a40c16f",
          "message": "No longer install protoc and flatc in CI (#616)",
          "timestamp": "2024-08-13T11:16:07-04:00",
          "tree_id": "2f40e4dd53b934fa36cbaade3e6be8dabca70c2a",
          "url": "https://github.com/spiraldb/vortex/commit/7267db497864f96655a631a34f1a36201a40c16f"
        },
        "date": 1723563986760,
        "tool": "cargo",
        "benches": [
          {
            "name": "tpch_q1/vortex-in-memory-no-pushdown",
            "value": 511555300,
            "range": "± 5750031",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-in-memory-pushdown",
            "value": 508943216,
            "range": "± 4432635",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/arrow",
            "value": 488961069,
            "range": "± 5202514",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/parquet",
            "value": 689469392,
            "range": "± 6313864",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-file-compressed",
            "value": 801983430,
            "range": "± 8348633",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-file-uncompressed",
            "value": 662845467,
            "range": "± 7061549",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-in-memory-no-pushdown",
            "value": 160990572,
            "range": "± 1720711",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-in-memory-pushdown",
            "value": 192936756,
            "range": "± 5151641",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/arrow",
            "value": 134044830,
            "range": "± 1811719",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/parquet",
            "value": 170643729,
            "range": "± 1775677",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-file-compressed",
            "value": 178535665,
            "range": "± 1582286",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-file-uncompressed",
            "value": 166893877,
            "range": "± 1714055",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-in-memory-no-pushdown",
            "value": 174723091,
            "range": "± 1982155",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-in-memory-pushdown",
            "value": 241017669,
            "range": "± 2240916",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/arrow",
            "value": 173748621,
            "range": "± 2398900",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/parquet",
            "value": 375353899,
            "range": "± 4403975",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-file-compressed",
            "value": 263650046,
            "range": "± 5564440",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-file-uncompressed",
            "value": 211384735,
            "range": "± 3280913",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-in-memory-no-pushdown",
            "value": 142876470,
            "range": "± 5686099",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-in-memory-pushdown",
            "value": 151287352,
            "range": "± 2277086",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/arrow",
            "value": 145168533,
            "range": "± 4020126",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/parquet",
            "value": 264013451,
            "range": "± 5158379",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-file-compressed",
            "value": 265634589,
            "range": "± 3814986",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-file-uncompressed",
            "value": 214520506,
            "range": "± 4028431",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-in-memory-no-pushdown",
            "value": 341470676,
            "range": "± 5087098",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-in-memory-pushdown",
            "value": 347015016,
            "range": "± 8518542",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/arrow",
            "value": 335030884,
            "range": "± 6583673",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/parquet",
            "value": 533023526,
            "range": "± 9735910",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-file-compressed",
            "value": 462408630,
            "range": "± 14601738",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-file-uncompressed",
            "value": 409754350,
            "range": "± 6315711",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-in-memory-no-pushdown",
            "value": 47798307,
            "range": "± 375024",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-in-memory-pushdown",
            "value": 209002704,
            "range": "± 2610328",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/arrow",
            "value": 42805936,
            "range": "± 174526",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/parquet",
            "value": 160009663,
            "range": "± 2058864",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-file-compressed",
            "value": 139874711,
            "range": "± 2257442",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-file-uncompressed",
            "value": 105816670,
            "range": "± 1795101",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-in-memory-no-pushdown",
            "value": 668290298,
            "range": "± 20142695",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-in-memory-pushdown",
            "value": 746725279,
            "range": "± 8792851",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/arrow",
            "value": 584964901,
            "range": "± 14369108",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/parquet",
            "value": 768938256,
            "range": "± 14414534",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-file-compressed",
            "value": 718511562,
            "range": "± 9262827",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-file-uncompressed",
            "value": 671499431,
            "range": "± 8513836",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-in-memory-no-pushdown",
            "value": 229948835,
            "range": "± 1521945",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-in-memory-pushdown",
            "value": 2123485718,
            "range": "± 42618337",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/arrow",
            "value": 242761159,
            "range": "± 7441828",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/parquet",
            "value": 498234367,
            "range": "± 5202828",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-file-compressed",
            "value": 321572984,
            "range": "± 5273695",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-file-uncompressed",
            "value": 270823137,
            "range": "± 1666529",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-in-memory-no-pushdown",
            "value": 441981603,
            "range": "± 5173028",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-in-memory-pushdown",
            "value": 438029117,
            "range": "± 5466324",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/arrow",
            "value": 428552047,
            "range": "± 4270232",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/parquet",
            "value": 720069082,
            "range": "± 6010563",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-file-compressed",
            "value": 567520501,
            "range": "± 17461358",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-file-uncompressed",
            "value": 522101379,
            "range": "± 10509806",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-in-memory-no-pushdown",
            "value": 259897237,
            "range": "± 3246961",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-in-memory-pushdown",
            "value": 359562581,
            "range": "± 2732073",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/arrow",
            "value": 253398505,
            "range": "± 6208086",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/parquet",
            "value": 505785355,
            "range": "± 2606741",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-file-compressed",
            "value": 479876818,
            "range": "± 979083",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-file-uncompressed",
            "value": 339620413,
            "range": "± 4472355",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-in-memory-no-pushdown",
            "value": 229631277,
            "range": "± 1263624",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-in-memory-pushdown",
            "value": 639481647,
            "range": "± 4422602",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/arrow",
            "value": 202925034,
            "range": "± 1655043",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/parquet",
            "value": 208073259,
            "range": "± 7267084",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-file-compressed",
            "value": 184528983,
            "range": "± 8167474",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-file-uncompressed",
            "value": 167901376,
            "range": "± 3145217",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-in-memory-no-pushdown",
            "value": 185121258,
            "range": "± 2605556",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-in-memory-pushdown",
            "value": 231069618,
            "range": "± 2014494",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/arrow",
            "value": 174496315,
            "range": "± 1444286",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/parquet",
            "value": 364018132,
            "range": "± 2836281",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-file-compressed",
            "value": 443040621,
            "range": "± 3458520",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-file-uncompressed",
            "value": 333528470,
            "range": "± 7656039",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-in-memory-no-pushdown",
            "value": 337499990,
            "range": "± 6844644",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-in-memory-pushdown",
            "value": 341221876,
            "range": "± 2142991",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/arrow",
            "value": 328994646,
            "range": "± 4844093",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/parquet",
            "value": 466528675,
            "range": "± 5533733",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-file-compressed",
            "value": 370520075,
            "range": "± 4404450",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-file-uncompressed",
            "value": 358938630,
            "range": "± 4493301",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-in-memory-no-pushdown",
            "value": 42878875,
            "range": "± 893159",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-in-memory-pushdown",
            "value": 41777139,
            "range": "± 352667",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/arrow",
            "value": 43773153,
            "range": "± 713399",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/parquet",
            "value": 223108111,
            "range": "± 1054769",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-file-compressed",
            "value": 131605380,
            "range": "± 2372704",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-file-uncompressed",
            "value": 108167232,
            "range": "± 2160415",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-in-memory-no-pushdown",
            "value": 126462135,
            "range": "± 384061",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-in-memory-pushdown",
            "value": 45619925,
            "range": "± 206296",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/arrow",
            "value": 110876934,
            "range": "± 676333",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/parquet",
            "value": 131014647,
            "range": "± 1453386",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-file-compressed",
            "value": 149216944,
            "range": "± 2142481",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-file-uncompressed",
            "value": 141926743,
            "range": "± 573331",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-in-memory-no-pushdown",
            "value": 739701563,
            "range": "± 36923074",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-in-memory-pushdown",
            "value": 1377205869,
            "range": "± 24205447",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/arrow",
            "value": 588974012,
            "range": "± 15613630",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/parquet",
            "value": 615239384,
            "range": "± 7419659",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-file-compressed",
            "value": 713773443,
            "range": "± 7830278",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-file-uncompressed",
            "value": 685274550,
            "range": "± 7451798",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-in-memory-no-pushdown",
            "value": 1178159097,
            "range": "± 31909905",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-in-memory-pushdown",
            "value": 1182746367,
            "range": "± 22777984",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/arrow",
            "value": 1150952053,
            "range": "± 20642675",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/parquet",
            "value": 1297126584,
            "range": "± 36592813",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-file-compressed",
            "value": 1298576965,
            "range": "± 14452494",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-file-uncompressed",
            "value": 1236330809,
            "range": "± 39612396",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-in-memory-no-pushdown",
            "value": 171896343,
            "range": "± 664150",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-in-memory-pushdown",
            "value": 548542245,
            "range": "± 5976002",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/arrow",
            "value": 160395708,
            "range": "± 1336578",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/parquet",
            "value": 470443915,
            "range": "± 2571656",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-file-compressed",
            "value": 1231702217,
            "range": "± 14335715",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-file-uncompressed",
            "value": 774818952,
            "range": "± 14014519",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-in-memory-no-pushdown",
            "value": 263034230,
            "range": "± 1885433",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-in-memory-pushdown",
            "value": 269081122,
            "range": "± 3882862",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/arrow",
            "value": 278429924,
            "range": "± 7531900",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/parquet",
            "value": 363481358,
            "range": "± 5529654",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-file-compressed",
            "value": 368233769,
            "range": "± 1680368",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-file-uncompressed",
            "value": 352277453,
            "range": "± 1889435",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-in-memory-no-pushdown",
            "value": 919211501,
            "range": "± 4024100",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-in-memory-pushdown",
            "value": 1621981324,
            "range": "± 23557570",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/arrow",
            "value": 911493286,
            "range": "± 2097610",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/parquet",
            "value": 1062936992,
            "range": "± 9401331",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-file-compressed",
            "value": 946983440,
            "range": "± 34283813",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-file-uncompressed",
            "value": 830185186,
            "range": "± 44537474",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-in-memory-no-pushdown",
            "value": 97582166,
            "range": "± 1379467",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-in-memory-pushdown",
            "value": 99120801,
            "range": "± 508881",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/arrow",
            "value": 68405164,
            "range": "± 734139",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/parquet",
            "value": 95149760,
            "range": "± 528256",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-file-compressed",
            "value": 114008055,
            "range": "± 1499300",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-file-uncompressed",
            "value": 113009287,
            "range": "± 647323",
            "unit": "ns/iter"
          }
        ]
      },
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
          "id": "18a788e1637f3f689d4b181a2eb4e40ff50febda",
          "message": "FoR compressor handles nullable arrays (#617)",
          "timestamp": "2024-08-13T17:32:39+01:00",
          "tree_id": "2b2f667bdf7953eff79cfc3163f3761a4be66c90",
          "url": "https://github.com/spiraldb/vortex/commit/18a788e1637f3f689d4b181a2eb4e40ff50febda"
        },
        "date": 1723568607824,
        "tool": "cargo",
        "benches": [
          {
            "name": "tpch_q1/vortex-in-memory-no-pushdown",
            "value": 484220919,
            "range": "± 5431913",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-in-memory-pushdown",
            "value": 485652388,
            "range": "± 3384265",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/arrow",
            "value": 470094418,
            "range": "± 2695909",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/parquet",
            "value": 674188396,
            "range": "± 9846313",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-file-compressed",
            "value": 785952042,
            "range": "± 6004422",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-file-uncompressed",
            "value": 631734953,
            "range": "± 2423651",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-in-memory-no-pushdown",
            "value": 149811411,
            "range": "± 2050880",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-in-memory-pushdown",
            "value": 174844695,
            "range": "± 2873462",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/arrow",
            "value": 129636255,
            "range": "± 1469011",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/parquet",
            "value": 162875667,
            "range": "± 754168",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-file-compressed",
            "value": 167813592,
            "range": "± 2327381",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-file-uncompressed",
            "value": 156248656,
            "range": "± 1282281",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-in-memory-no-pushdown",
            "value": 164765233,
            "range": "± 2961376",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-in-memory-pushdown",
            "value": 219215051,
            "range": "± 2405893",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/arrow",
            "value": 160969078,
            "range": "± 1031195",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/parquet",
            "value": 352144886,
            "range": "± 2101868",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-file-compressed",
            "value": 246478290,
            "range": "± 3441460",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-file-uncompressed",
            "value": 198553446,
            "range": "± 3600245",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-in-memory-no-pushdown",
            "value": 132540610,
            "range": "± 1529277",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-in-memory-pushdown",
            "value": 133600615,
            "range": "± 1739121",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/arrow",
            "value": 124758146,
            "range": "± 2499073",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/parquet",
            "value": 246573897,
            "range": "± 4051065",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-file-compressed",
            "value": 247762538,
            "range": "± 4821490",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-file-uncompressed",
            "value": 201442715,
            "range": "± 1190617",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-in-memory-no-pushdown",
            "value": 329736532,
            "range": "± 4775326",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-in-memory-pushdown",
            "value": 332228259,
            "range": "± 4513705",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/arrow",
            "value": 333530682,
            "range": "± 5083038",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/parquet",
            "value": 494407882,
            "range": "± 1975010",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-file-compressed",
            "value": 424056648,
            "range": "± 6810019",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-file-uncompressed",
            "value": 379881869,
            "range": "± 7079253",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-in-memory-no-pushdown",
            "value": 42233963,
            "range": "± 169790",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-in-memory-pushdown",
            "value": 192296629,
            "range": "± 2003466",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/arrow",
            "value": 38316327,
            "range": "± 466812",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/parquet",
            "value": 149438796,
            "range": "± 499196",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-file-compressed",
            "value": 121750556,
            "range": "± 1354926",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-file-uncompressed",
            "value": 96590325,
            "range": "± 2094092",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-in-memory-no-pushdown",
            "value": 601115963,
            "range": "± 5964522",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-in-memory-pushdown",
            "value": 758267289,
            "range": "± 14013232",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/arrow",
            "value": 615829062,
            "range": "± 16973152",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/parquet",
            "value": 782503807,
            "range": "± 17061595",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-file-compressed",
            "value": 771958886,
            "range": "± 15716627",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-file-uncompressed",
            "value": 732528112,
            "range": "± 21935565",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-in-memory-no-pushdown",
            "value": 236500070,
            "range": "± 2199123",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-in-memory-pushdown",
            "value": 2443930443,
            "range": "± 74882540",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/arrow",
            "value": 234583504,
            "range": "± 3500688",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/parquet",
            "value": 515097727,
            "range": "± 3295236",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-file-compressed",
            "value": 330833461,
            "range": "± 4775555",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-file-uncompressed",
            "value": 264914435,
            "range": "± 7044006",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-in-memory-no-pushdown",
            "value": 476805886,
            "range": "± 10297270",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-in-memory-pushdown",
            "value": 479737896,
            "range": "± 5438832",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/arrow",
            "value": 475570561,
            "range": "± 11197872",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/parquet",
            "value": 761335048,
            "range": "± 12523745",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-file-compressed",
            "value": 647326702,
            "range": "± 10548328",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-file-uncompressed",
            "value": 560908358,
            "range": "± 2801746",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-in-memory-no-pushdown",
            "value": 266453119,
            "range": "± 1375795",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-in-memory-pushdown",
            "value": 364718641,
            "range": "± 3007049",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/arrow",
            "value": 259003061,
            "range": "± 1978610",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/parquet",
            "value": 518508281,
            "range": "± 4270089",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-file-compressed",
            "value": 495992409,
            "range": "± 3877161",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-file-uncompressed",
            "value": 351006425,
            "range": "± 1854295",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-in-memory-no-pushdown",
            "value": 230959122,
            "range": "± 2673625",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-in-memory-pushdown",
            "value": 634113223,
            "range": "± 8663026",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/arrow",
            "value": 184007019,
            "range": "± 4944222",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/parquet",
            "value": 203936042,
            "range": "± 4167881",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-file-compressed",
            "value": 192166748,
            "range": "± 4936217",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-file-uncompressed",
            "value": 194776854,
            "range": "± 6733682",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-in-memory-no-pushdown",
            "value": 185829992,
            "range": "± 1004150",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-in-memory-pushdown",
            "value": 236324719,
            "range": "± 2225696",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/arrow",
            "value": 176034299,
            "range": "± 1378499",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/parquet",
            "value": 367144880,
            "range": "± 4232095",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-file-compressed",
            "value": 449811070,
            "range": "± 2041208",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-file-uncompressed",
            "value": 343992801,
            "range": "± 838534",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-in-memory-no-pushdown",
            "value": 360696487,
            "range": "± 1206713",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-in-memory-pushdown",
            "value": 362585482,
            "range": "± 4822439",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/arrow",
            "value": 360097141,
            "range": "± 3451294",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/parquet",
            "value": 504815322,
            "range": "± 4054052",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-file-compressed",
            "value": 379577918,
            "range": "± 3380832",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-file-uncompressed",
            "value": 373666104,
            "range": "± 2757939",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-in-memory-no-pushdown",
            "value": 44421183,
            "range": "± 415370",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-in-memory-pushdown",
            "value": 45096553,
            "range": "± 491783",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/arrow",
            "value": 43167935,
            "range": "± 271713",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/parquet",
            "value": 227907114,
            "range": "± 2727635",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-file-compressed",
            "value": 127872940,
            "range": "± 753066",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-file-uncompressed",
            "value": 108740449,
            "range": "± 950659",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-in-memory-no-pushdown",
            "value": 132079345,
            "range": "± 814160",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-in-memory-pushdown",
            "value": 47912469,
            "range": "± 338130",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/arrow",
            "value": 116818930,
            "range": "± 1227944",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/parquet",
            "value": 135866440,
            "range": "± 731324",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-file-compressed",
            "value": 151605873,
            "range": "± 386208",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-file-uncompressed",
            "value": 144538059,
            "range": "± 1012302",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-in-memory-no-pushdown",
            "value": 748223143,
            "range": "± 19399185",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-in-memory-pushdown",
            "value": 1522925974,
            "range": "± 85500219",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/arrow",
            "value": 716856441,
            "range": "± 18358353",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/parquet",
            "value": 642849002,
            "range": "± 8154637",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-file-compressed",
            "value": 767180980,
            "range": "± 13726816",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-file-uncompressed",
            "value": 725234420,
            "range": "± 9829983",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-in-memory-no-pushdown",
            "value": 1275181095,
            "range": "± 33741569",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-in-memory-pushdown",
            "value": 1318821321,
            "range": "± 14543003",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/arrow",
            "value": 1285504896,
            "range": "± 26763504",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/parquet",
            "value": 1451991027,
            "range": "± 17121555",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-file-compressed",
            "value": 1366153138,
            "range": "± 21575634",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-file-uncompressed",
            "value": 1302074022,
            "range": "± 31097207",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-in-memory-no-pushdown",
            "value": 174162512,
            "range": "± 938726",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-in-memory-pushdown",
            "value": 557739258,
            "range": "± 4840619",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/arrow",
            "value": 158072566,
            "range": "± 396641",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/parquet",
            "value": 476743449,
            "range": "± 4009460",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-file-compressed",
            "value": 1239939672,
            "range": "± 3225762",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-file-uncompressed",
            "value": 780187991,
            "range": "± 1978294",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-in-memory-no-pushdown",
            "value": 271498304,
            "range": "± 7948927",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-in-memory-pushdown",
            "value": 276932655,
            "range": "± 3115689",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/arrow",
            "value": 257024021,
            "range": "± 7175281",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/parquet",
            "value": 373230121,
            "range": "± 10098068",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-file-compressed",
            "value": 378269800,
            "range": "± 13762410",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-file-uncompressed",
            "value": 365002049,
            "range": "± 14623506",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-in-memory-no-pushdown",
            "value": 947994781,
            "range": "± 19941936",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-in-memory-pushdown",
            "value": 1644352233,
            "range": "± 6744363",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/arrow",
            "value": 938970139,
            "range": "± 14779777",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/parquet",
            "value": 1107682078,
            "range": "± 10913353",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-file-compressed",
            "value": 938467145,
            "range": "± 7286871",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-file-uncompressed",
            "value": 825004235,
            "range": "± 5295793",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-in-memory-no-pushdown",
            "value": 100699331,
            "range": "± 775989",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-in-memory-pushdown",
            "value": 100813283,
            "range": "± 430715",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/arrow",
            "value": 68459284,
            "range": "± 321350",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/parquet",
            "value": 100910403,
            "range": "± 681845",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-file-compressed",
            "value": 117193513,
            "range": "± 1805912",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-file-uncompressed",
            "value": 116940517,
            "range": "± 1059499",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "adam@spiraldb.com",
            "name": "Adam Gutglick",
            "username": "AdamGS"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "7dd5ac3b59537e063719000c181245009c580d71",
          "message": "Fix a bug in vortex in-memory predicate pushdown (#618)",
          "timestamp": "2024-08-14T11:47:06+01:00",
          "tree_id": "30f45d45f2c91939381cb659a1abd4d3adf5bc5a",
          "url": "https://github.com/spiraldb/vortex/commit/7dd5ac3b59537e063719000c181245009c580d71"
        },
        "date": 1723634338209,
        "tool": "cargo",
        "benches": [
          {
            "name": "tpch_q1/vortex-in-memory-no-pushdown",
            "value": 486753981,
            "range": "± 3156078",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-in-memory-pushdown",
            "value": 491069854,
            "range": "± 2612711",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/arrow",
            "value": 476727567,
            "range": "± 2634614",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/parquet",
            "value": 678306502,
            "range": "± 3047628",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-file-compressed",
            "value": 781625511,
            "range": "± 2015200",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-file-uncompressed",
            "value": 642204301,
            "range": "± 8290991",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-in-memory-no-pushdown",
            "value": 149657214,
            "range": "± 1411720",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-in-memory-pushdown",
            "value": 178210917,
            "range": "± 547118",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/arrow",
            "value": 130366637,
            "range": "± 893897",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/parquet",
            "value": 163931620,
            "range": "± 1150026",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-file-compressed",
            "value": 171100053,
            "range": "± 2649267",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-file-uncompressed",
            "value": 163943669,
            "range": "± 785089",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-in-memory-no-pushdown",
            "value": 172070275,
            "range": "± 1418629",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-in-memory-pushdown",
            "value": 214435189,
            "range": "± 1296793",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/arrow",
            "value": 157827699,
            "range": "± 1519536",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/parquet",
            "value": 365568742,
            "range": "± 6020804",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-file-compressed",
            "value": 266827064,
            "range": "± 3918888",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-file-uncompressed",
            "value": 207166015,
            "range": "± 1316659",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-in-memory-no-pushdown",
            "value": 135810661,
            "range": "± 1252919",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-in-memory-pushdown",
            "value": 135124752,
            "range": "± 1472897",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/arrow",
            "value": 130131794,
            "range": "± 1053693",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/parquet",
            "value": 245899981,
            "range": "± 1644058",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-file-compressed",
            "value": 257577966,
            "range": "± 2943687",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-file-uncompressed",
            "value": 211910105,
            "range": "± 2659903",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-in-memory-no-pushdown",
            "value": 340145379,
            "range": "± 2297469",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-in-memory-pushdown",
            "value": 342985912,
            "range": "± 1247373",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/arrow",
            "value": 336544207,
            "range": "± 3178956",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/parquet",
            "value": 506566424,
            "range": "± 5251892",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-file-compressed",
            "value": 443845480,
            "range": "± 2162718",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-file-uncompressed",
            "value": 389397580,
            "range": "± 4082060",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-in-memory-no-pushdown",
            "value": 43751766,
            "range": "± 123066",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-in-memory-pushdown",
            "value": 197384900,
            "range": "± 1128634",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/arrow",
            "value": 36740032,
            "range": "± 238073",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/parquet",
            "value": 149604491,
            "range": "± 322269",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-file-compressed",
            "value": 123014372,
            "range": "± 2912777",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-file-uncompressed",
            "value": 97048266,
            "range": "± 2162092",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-in-memory-no-pushdown",
            "value": 574509183,
            "range": "± 3550313",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-in-memory-pushdown",
            "value": 730853042,
            "range": "± 11167599",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/arrow",
            "value": 567779060,
            "range": "± 3667091",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/parquet",
            "value": 770942050,
            "range": "± 19929415",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-file-compressed",
            "value": 700666068,
            "range": "± 3889032",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-file-uncompressed",
            "value": 653839955,
            "range": "± 6653240",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-in-memory-no-pushdown",
            "value": 224711974,
            "range": "± 1462915",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-in-memory-pushdown",
            "value": 2028387745,
            "range": "± 16518103",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/arrow",
            "value": 223933741,
            "range": "± 1439798",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/parquet",
            "value": 484706493,
            "range": "± 3754232",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-file-compressed",
            "value": 317515844,
            "range": "± 5798833",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-file-uncompressed",
            "value": 249418797,
            "range": "± 1081510",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-in-memory-no-pushdown",
            "value": 416835501,
            "range": "± 2031599",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-in-memory-pushdown",
            "value": 421539236,
            "range": "± 3762243",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/arrow",
            "value": 424519356,
            "range": "± 5941192",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/parquet",
            "value": 736299046,
            "range": "± 14719351",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-file-compressed",
            "value": 578278964,
            "range": "± 20272248",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-file-uncompressed",
            "value": 482925079,
            "range": "± 11456415",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-in-memory-no-pushdown",
            "value": 242901782,
            "range": "± 1789798",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-in-memory-pushdown",
            "value": 343525362,
            "range": "± 9443055",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/arrow",
            "value": 236938664,
            "range": "± 1405504",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/parquet",
            "value": 488168612,
            "range": "± 3836777",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-file-compressed",
            "value": 476396157,
            "range": "± 4512906",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-file-uncompressed",
            "value": 336979704,
            "range": "± 1732349",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-in-memory-no-pushdown",
            "value": 223445739,
            "range": "± 809023",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-in-memory-pushdown",
            "value": 613551202,
            "range": "± 4834731",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/arrow",
            "value": 179630886,
            "range": "± 688975",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/parquet",
            "value": 192515182,
            "range": "± 941491",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-file-compressed",
            "value": 177647755,
            "range": "± 2071062",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-file-uncompressed",
            "value": 171030797,
            "range": "± 3171910",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-in-memory-no-pushdown",
            "value": 182800840,
            "range": "± 1455145",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-in-memory-pushdown",
            "value": 318307145,
            "range": "± 1486468",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/arrow",
            "value": 172792715,
            "range": "± 621353",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/parquet",
            "value": 355441585,
            "range": "± 3618576",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-file-compressed",
            "value": 432909636,
            "range": "± 1828995",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-file-uncompressed",
            "value": 335796944,
            "range": "± 7533826",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-in-memory-no-pushdown",
            "value": 322917532,
            "range": "± 4557947",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-in-memory-pushdown",
            "value": 328687356,
            "range": "± 7630248",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/arrow",
            "value": 314492607,
            "range": "± 3976741",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/parquet",
            "value": 450378160,
            "range": "± 5268909",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-file-compressed",
            "value": 352347437,
            "range": "± 4835312",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-file-uncompressed",
            "value": 345938984,
            "range": "± 1624761",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-in-memory-no-pushdown",
            "value": 38809898,
            "range": "± 446156",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-in-memory-pushdown",
            "value": 39265122,
            "range": "± 127442",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/arrow",
            "value": 39876059,
            "range": "± 441641",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/parquet",
            "value": 218936518,
            "range": "± 868287",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-file-compressed",
            "value": 127971191,
            "range": "± 1195632",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-file-uncompressed",
            "value": 106015370,
            "range": "± 1043093",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-in-memory-no-pushdown",
            "value": 124599905,
            "range": "± 947906",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-in-memory-pushdown",
            "value": 44881459,
            "range": "± 174957",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/arrow",
            "value": 111787536,
            "range": "± 1579603",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/parquet",
            "value": 131486243,
            "range": "± 2621202",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-file-compressed",
            "value": 144101507,
            "range": "± 420069",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-file-uncompressed",
            "value": 143750013,
            "range": "± 553807",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-in-memory-no-pushdown",
            "value": 765565239,
            "range": "± 23433787",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-in-memory-pushdown",
            "value": 1539194664,
            "range": "± 35376314",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/arrow",
            "value": 678286454,
            "range": "± 11660675",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/parquet",
            "value": 664681185,
            "range": "± 8101302",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-file-compressed",
            "value": 755397050,
            "range": "± 21693295",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-file-uncompressed",
            "value": 705760171,
            "range": "± 8286281",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-in-memory-no-pushdown",
            "value": 1283663131,
            "range": "± 11303269",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-in-memory-pushdown",
            "value": 1295109501,
            "range": "± 26867886",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/arrow",
            "value": 1272053120,
            "range": "± 23433412",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/parquet",
            "value": 1448040617,
            "range": "± 12601523",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-file-compressed",
            "value": 1385532756,
            "range": "± 11303674",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-file-uncompressed",
            "value": 1277578220,
            "range": "± 20910246",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-in-memory-no-pushdown",
            "value": 174813442,
            "range": "± 2226060",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-in-memory-pushdown",
            "value": 540580677,
            "range": "± 1957693",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/arrow",
            "value": 157524926,
            "range": "± 519661",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/parquet",
            "value": 471556306,
            "range": "± 1039404",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-file-compressed",
            "value": 1214615127,
            "range": "± 16294426",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-file-uncompressed",
            "value": 780950411,
            "range": "± 3951963",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-in-memory-no-pushdown",
            "value": 275088521,
            "range": "± 13184664",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-in-memory-pushdown",
            "value": 286806724,
            "range": "± 12504467",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/arrow",
            "value": 252383849,
            "range": "± 2405118",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/parquet",
            "value": 364381771,
            "range": "± 5783565",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-file-compressed",
            "value": 365346341,
            "range": "± 3006421",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-file-uncompressed",
            "value": 354023489,
            "range": "± 3161107",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-in-memory-no-pushdown",
            "value": 941877052,
            "range": "± 7737352",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-in-memory-pushdown",
            "value": 1608210739,
            "range": "± 6571769",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/arrow",
            "value": 923535167,
            "range": "± 12691993",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/parquet",
            "value": 1075388444,
            "range": "± 12238074",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-file-compressed",
            "value": 918282937,
            "range": "± 5652683",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-file-uncompressed",
            "value": 803278268,
            "range": "± 6128056",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-in-memory-no-pushdown",
            "value": 98207285,
            "range": "± 1193253",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-in-memory-pushdown",
            "value": 96933766,
            "range": "± 825842",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/arrow",
            "value": 67773692,
            "range": "± 435489",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/parquet",
            "value": 99219783,
            "range": "± 578934",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-file-compressed",
            "value": 110633294,
            "range": "± 462280",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-file-uncompressed",
            "value": 110133101,
            "range": "± 813496",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "adam@spiraldb.com",
            "name": "Adam Gutglick",
            "username": "AdamGS"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "664ffb659f9ae51eecb75369666adfbd9f504405",
          "message": "Fix bug where operations were negated instead of swapped when lhs/rhs were flipped (#619)",
          "timestamp": "2024-08-14T12:27:09Z",
          "tree_id": "2c22e6ecf52a794ac80da565d41edba6e1e43dda",
          "url": "https://github.com/spiraldb/vortex/commit/664ffb659f9ae51eecb75369666adfbd9f504405"
        },
        "date": 1723640224421,
        "tool": "cargo",
        "benches": [
          {
            "name": "tpch_q1/vortex-in-memory-no-pushdown",
            "value": 494900955,
            "range": "± 8067775",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-in-memory-pushdown",
            "value": 499330251,
            "range": "± 8964729",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/arrow",
            "value": 470686433,
            "range": "± 5814838",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/parquet",
            "value": 674364500,
            "range": "± 1899466",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-file-compressed",
            "value": 784765195,
            "range": "± 7113057",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-file-uncompressed",
            "value": 638725747,
            "range": "± 2335162",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-in-memory-no-pushdown",
            "value": 150494698,
            "range": "± 1955215",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-in-memory-pushdown",
            "value": 155894277,
            "range": "± 2262931",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/arrow",
            "value": 129143856,
            "range": "± 268352",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/parquet",
            "value": 162771712,
            "range": "± 816706",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-file-compressed",
            "value": 185451330,
            "range": "± 2247261",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-file-uncompressed",
            "value": 173932475,
            "range": "± 983150",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-in-memory-no-pushdown",
            "value": 165512269,
            "range": "± 3136786",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-in-memory-pushdown",
            "value": 166207125,
            "range": "± 3558681",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/arrow",
            "value": 159237397,
            "range": "± 811538",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/parquet",
            "value": 347235252,
            "range": "± 1253534",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-file-compressed",
            "value": 276464140,
            "range": "± 3207026",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-file-uncompressed",
            "value": 228204498,
            "range": "± 2387488",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-in-memory-no-pushdown",
            "value": 128528313,
            "range": "± 2092535",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-in-memory-pushdown",
            "value": 125805789,
            "range": "± 451943",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/arrow",
            "value": 120470837,
            "range": "± 349006",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/parquet",
            "value": 234214134,
            "range": "± 2901515",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-file-compressed",
            "value": 243857977,
            "range": "± 3582701",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-file-uncompressed",
            "value": 203880324,
            "range": "± 3003319",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-in-memory-no-pushdown",
            "value": 312087041,
            "range": "± 1520199",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-in-memory-pushdown",
            "value": 316209542,
            "range": "± 2285113",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/arrow",
            "value": 321358984,
            "range": "± 10108195",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/parquet",
            "value": 459226898,
            "range": "± 2912817",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-file-compressed",
            "value": 405354496,
            "range": "± 8439362",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-file-uncompressed",
            "value": 372175089,
            "range": "± 5285919",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-in-memory-no-pushdown",
            "value": 40893579,
            "range": "± 418006",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-in-memory-pushdown",
            "value": 199770668,
            "range": "± 3007643",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/arrow",
            "value": 38043339,
            "range": "± 434389",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/parquet",
            "value": 150263437,
            "range": "± 1126933",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-file-compressed",
            "value": 126422646,
            "range": "± 960141",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-file-uncompressed",
            "value": 99769723,
            "range": "± 1020488",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-in-memory-no-pushdown",
            "value": 584303757,
            "range": "± 11384927",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-in-memory-pushdown",
            "value": 581479038,
            "range": "± 3510162",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/arrow",
            "value": 576087100,
            "range": "± 1808581",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/parquet",
            "value": 750158311,
            "range": "± 7719641",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-file-compressed",
            "value": 742622539,
            "range": "± 16784170",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-file-uncompressed",
            "value": 649348371,
            "range": "± 8831848",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-in-memory-no-pushdown",
            "value": 226614818,
            "range": "± 1057477",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-in-memory-pushdown",
            "value": 234807226,
            "range": "± 4835809",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/arrow",
            "value": 229986913,
            "range": "± 1162740",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/parquet",
            "value": 490736265,
            "range": "± 4039031",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-file-compressed",
            "value": 335786651,
            "range": "± 2082628",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-file-uncompressed",
            "value": 295602739,
            "range": "± 2459062",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-in-memory-no-pushdown",
            "value": 424516835,
            "range": "± 2061387",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-in-memory-pushdown",
            "value": 422308041,
            "range": "± 1436201",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/arrow",
            "value": 422407318,
            "range": "± 4867337",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/parquet",
            "value": 723982077,
            "range": "± 6342070",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-file-compressed",
            "value": 594242061,
            "range": "± 4853722",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-file-uncompressed",
            "value": 526895757,
            "range": "± 6368650",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-in-memory-no-pushdown",
            "value": 248279762,
            "range": "± 1588289",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-in-memory-pushdown",
            "value": 260018894,
            "range": "± 2373119",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/arrow",
            "value": 237995113,
            "range": "± 4914849",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/parquet",
            "value": 498149640,
            "range": "± 5315179",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-file-compressed",
            "value": 590510825,
            "range": "± 3028906",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-file-uncompressed",
            "value": 455002810,
            "range": "± 3495152",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-in-memory-no-pushdown",
            "value": 228628980,
            "range": "± 4525994",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-in-memory-pushdown",
            "value": 220469431,
            "range": "± 4191207",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/arrow",
            "value": 181889274,
            "range": "± 1236559",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/parquet",
            "value": 205825367,
            "range": "± 1163619",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-file-compressed",
            "value": 239418180,
            "range": "± 4412550",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-file-uncompressed",
            "value": 233857186,
            "range": "± 2362934",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-in-memory-no-pushdown",
            "value": 181255752,
            "range": "± 411051",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-in-memory-pushdown",
            "value": 221669613,
            "range": "± 277649",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/arrow",
            "value": 171346707,
            "range": "± 236631",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/parquet",
            "value": 354657146,
            "range": "± 2289141",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-file-compressed",
            "value": 439346828,
            "range": "± 5552071",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-file-uncompressed",
            "value": 334501250,
            "range": "± 1771077",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-in-memory-no-pushdown",
            "value": 334535252,
            "range": "± 4469006",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-in-memory-pushdown",
            "value": 340908544,
            "range": "± 6826913",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/arrow",
            "value": 348145992,
            "range": "± 1762470",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/parquet",
            "value": 476993120,
            "range": "± 5484807",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-file-compressed",
            "value": 359314865,
            "range": "± 5342210",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-file-uncompressed",
            "value": 358248072,
            "range": "± 4821693",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-in-memory-no-pushdown",
            "value": 40206543,
            "range": "± 974878",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-in-memory-pushdown",
            "value": 40803345,
            "range": "± 825656",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/arrow",
            "value": 42291845,
            "range": "± 1025100",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/parquet",
            "value": 223654213,
            "range": "± 2501441",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-file-compressed",
            "value": 125755719,
            "range": "± 1585226",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-file-uncompressed",
            "value": 109013218,
            "range": "± 2391289",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-in-memory-no-pushdown",
            "value": 124136793,
            "range": "± 412632",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-in-memory-pushdown",
            "value": 128514870,
            "range": "± 860621",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/arrow",
            "value": 110982657,
            "range": "± 708877",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/parquet",
            "value": 129747746,
            "range": "± 1598551",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-file-compressed",
            "value": 148252265,
            "range": "± 1544536",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-file-uncompressed",
            "value": 143638260,
            "range": "± 1725476",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-in-memory-no-pushdown",
            "value": 655333554,
            "range": "± 32132335",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-in-memory-pushdown",
            "value": 643956541,
            "range": "± 30182760",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/arrow",
            "value": 566653285,
            "range": "± 22128702",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/parquet",
            "value": 608683643,
            "range": "± 11539501",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-file-compressed",
            "value": 714186313,
            "range": "± 16034742",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-file-uncompressed",
            "value": 656465569,
            "range": "± 16841768",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-in-memory-no-pushdown",
            "value": 1209279111,
            "range": "± 27916926",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-in-memory-pushdown",
            "value": 1086454295,
            "range": "± 13100269",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/arrow",
            "value": 1088656686,
            "range": "± 17770664",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/parquet",
            "value": 1268435268,
            "range": "± 20788101",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-file-compressed",
            "value": 1250522128,
            "range": "± 33524595",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-file-uncompressed",
            "value": 1177019092,
            "range": "± 23486646",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-in-memory-no-pushdown",
            "value": 171500213,
            "range": "± 1110884",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-in-memory-pushdown",
            "value": 450720292,
            "range": "± 4943092",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/arrow",
            "value": 157449333,
            "range": "± 566686",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/parquet",
            "value": 473669639,
            "range": "± 927340",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-file-compressed",
            "value": 1096359199,
            "range": "± 4736585",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-file-uncompressed",
            "value": 665487276,
            "range": "± 9362835",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-in-memory-no-pushdown",
            "value": 303021050,
            "range": "± 4862863",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-in-memory-pushdown",
            "value": 302834217,
            "range": "± 9208268",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/arrow",
            "value": 269196133,
            "range": "± 7941206",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/parquet",
            "value": 389197351,
            "range": "± 6610708",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-file-compressed",
            "value": 382522414,
            "range": "± 8697881",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-file-uncompressed",
            "value": 364303130,
            "range": "± 3371940",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-in-memory-no-pushdown",
            "value": 956465234,
            "range": "± 7476542",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-in-memory-pushdown",
            "value": 966818615,
            "range": "± 4590542",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/arrow",
            "value": 927117175,
            "range": "± 6817651",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/parquet",
            "value": 1123851350,
            "range": "± 25095192",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-file-compressed",
            "value": 1252524336,
            "range": "± 22758526",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-file-uncompressed",
            "value": 1161413750,
            "range": "± 17462689",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-in-memory-no-pushdown",
            "value": 102248287,
            "range": "± 723812",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-in-memory-pushdown",
            "value": 102863420,
            "range": "± 1290657",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/arrow",
            "value": 68195628,
            "range": "± 601271",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/parquet",
            "value": 99109887,
            "range": "± 790278",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-file-compressed",
            "value": 116575066,
            "range": "± 442468",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-file-uncompressed",
            "value": 117232734,
            "range": "± 1681950",
            "unit": "ns/iter"
          }
        ]
      }
    ]
  }
}