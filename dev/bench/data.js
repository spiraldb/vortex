window.BENCHMARK_DATA = {
  "lastUpdate": 1723548513992,
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
      }
    ]
  }
}