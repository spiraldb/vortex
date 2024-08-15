window.BENCHMARK_DATA = {
  "lastUpdate": 1723743774237,
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
          "id": "b6508745be63148ca3ab9f933beba2486c584968",
          "message": "Assert expected row count in tpch_benchmark binary (#620)\n\nWill add similar logic in the benchmark in follow up",
          "timestamp": "2024-08-14T13:34:18Z",
          "tree_id": "159f07bcb9636c2c69d08068f89365a0efb4597b",
          "url": "https://github.com/spiraldb/vortex/commit/b6508745be63148ca3ab9f933beba2486c584968"
        },
        "date": 1723644246394,
        "tool": "cargo",
        "benches": [
          {
            "name": "tpch_q1/vortex-in-memory-no-pushdown",
            "value": 488487819,
            "range": "± 2573816",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-in-memory-pushdown",
            "value": 489521374,
            "range": "± 3094567",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/arrow",
            "value": 475084960,
            "range": "± 3980148",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/parquet",
            "value": 676276779,
            "range": "± 3059262",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-file-compressed",
            "value": 783758456,
            "range": "± 6829380",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-file-uncompressed",
            "value": 639096534,
            "range": "± 3986177",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-in-memory-no-pushdown",
            "value": 149914779,
            "range": "± 1972609",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-in-memory-pushdown",
            "value": 149731102,
            "range": "± 800996",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/arrow",
            "value": 127351403,
            "range": "± 785992",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/parquet",
            "value": 162718571,
            "range": "± 935102",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-file-compressed",
            "value": 189682692,
            "range": "± 1907580",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-file-uncompressed",
            "value": 173344536,
            "range": "± 1284836",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-in-memory-no-pushdown",
            "value": 164175091,
            "range": "± 2087353",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-in-memory-pushdown",
            "value": 165455839,
            "range": "± 5001785",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/arrow",
            "value": 165186794,
            "range": "± 2940868",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/parquet",
            "value": 351817325,
            "range": "± 3303411",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-file-compressed",
            "value": 281506608,
            "range": "± 3438865",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-file-uncompressed",
            "value": 224663324,
            "range": "± 3443017",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-in-memory-no-pushdown",
            "value": 129256491,
            "range": "± 1046682",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-in-memory-pushdown",
            "value": 126998515,
            "range": "± 1449857",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/arrow",
            "value": 123399021,
            "range": "± 1471846",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/parquet",
            "value": 236875868,
            "range": "± 3481716",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-file-compressed",
            "value": 250333079,
            "range": "± 3956081",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-file-uncompressed",
            "value": 198488690,
            "range": "± 4153111",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-in-memory-no-pushdown",
            "value": 314111017,
            "range": "± 3084531",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-in-memory-pushdown",
            "value": 316405652,
            "range": "± 6214411",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/arrow",
            "value": 311016217,
            "range": "± 5037724",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/parquet",
            "value": 471874821,
            "range": "± 4152644",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-file-compressed",
            "value": 404344272,
            "range": "± 9242599",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-file-uncompressed",
            "value": 353216984,
            "range": "± 7223073",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-in-memory-no-pushdown",
            "value": 41203570,
            "range": "± 810919",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-in-memory-pushdown",
            "value": 194493137,
            "range": "± 349076",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/arrow",
            "value": 37027669,
            "range": "± 714243",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/parquet",
            "value": 149925154,
            "range": "± 911777",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-file-compressed",
            "value": 123802136,
            "range": "± 1087732",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-file-uncompressed",
            "value": 96808218,
            "range": "± 3706390",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-in-memory-no-pushdown",
            "value": 580169917,
            "range": "± 9049450",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-in-memory-pushdown",
            "value": 589053223,
            "range": "± 7715084",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/arrow",
            "value": 593484117,
            "range": "± 11021304",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/parquet",
            "value": 756012806,
            "range": "± 18993088",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-file-compressed",
            "value": 721722888,
            "range": "± 8596436",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-file-uncompressed",
            "value": 663685396,
            "range": "± 12234395",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-in-memory-no-pushdown",
            "value": 223654418,
            "range": "± 1807727",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-in-memory-pushdown",
            "value": 224652474,
            "range": "± 888675",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/arrow",
            "value": 223470448,
            "range": "± 2233698",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/parquet",
            "value": 492821597,
            "range": "± 4829384",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-file-compressed",
            "value": 342639739,
            "range": "± 4984810",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-file-uncompressed",
            "value": 296642941,
            "range": "± 2736087",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-in-memory-no-pushdown",
            "value": 420247215,
            "range": "± 3072043",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-in-memory-pushdown",
            "value": 417940721,
            "range": "± 3340023",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/arrow",
            "value": 414905934,
            "range": "± 5870249",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/parquet",
            "value": 711770177,
            "range": "± 3581153",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-file-compressed",
            "value": 557761046,
            "range": "± 7452784",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-file-uncompressed",
            "value": 501472364,
            "range": "± 6688459",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-in-memory-no-pushdown",
            "value": 244236804,
            "range": "± 1758446",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-in-memory-pushdown",
            "value": 259661484,
            "range": "± 1861644",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/arrow",
            "value": 240771412,
            "range": "± 3367681",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/parquet",
            "value": 495648831,
            "range": "± 6690056",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-file-compressed",
            "value": 601229415,
            "range": "± 3342467",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-file-uncompressed",
            "value": 453006240,
            "range": "± 3085745",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-in-memory-no-pushdown",
            "value": 223439871,
            "range": "± 2197359",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-in-memory-pushdown",
            "value": 221439516,
            "range": "± 3869335",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/arrow",
            "value": 180846747,
            "range": "± 1689714",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/parquet",
            "value": 197000748,
            "range": "± 3138584",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-file-compressed",
            "value": 240357845,
            "range": "± 3024165",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-file-uncompressed",
            "value": 233149799,
            "range": "± 3761421",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-in-memory-no-pushdown",
            "value": 185686488,
            "range": "± 1086631",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-in-memory-pushdown",
            "value": 224817657,
            "range": "± 1525475",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/arrow",
            "value": 171851748,
            "range": "± 905429",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/parquet",
            "value": 357576854,
            "range": "± 3517849",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-file-compressed",
            "value": 437729560,
            "range": "± 2154132",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-file-uncompressed",
            "value": 331649615,
            "range": "± 2093107",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-in-memory-no-pushdown",
            "value": 333356946,
            "range": "± 2366236",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-in-memory-pushdown",
            "value": 333041770,
            "range": "± 2963417",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/arrow",
            "value": 331884458,
            "range": "± 2955907",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/parquet",
            "value": 476437093,
            "range": "± 10021896",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-file-compressed",
            "value": 356639741,
            "range": "± 5371430",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-file-uncompressed",
            "value": 355892342,
            "range": "± 2675502",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-in-memory-no-pushdown",
            "value": 40224882,
            "range": "± 454428",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-in-memory-pushdown",
            "value": 40939433,
            "range": "± 440972",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/arrow",
            "value": 40570096,
            "range": "± 785178",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/parquet",
            "value": 221657043,
            "range": "± 1165590",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-file-compressed",
            "value": 128656333,
            "range": "± 1652074",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-file-uncompressed",
            "value": 104964410,
            "range": "± 2285574",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-in-memory-no-pushdown",
            "value": 126233567,
            "range": "± 621663",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-in-memory-pushdown",
            "value": 130665332,
            "range": "± 654930",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/arrow",
            "value": 111384496,
            "range": "± 1146597",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/parquet",
            "value": 128524108,
            "range": "± 551545",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-file-compressed",
            "value": 146855826,
            "range": "± 708599",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-file-uncompressed",
            "value": 140056111,
            "range": "± 752168",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-in-memory-no-pushdown",
            "value": 642468553,
            "range": "± 19158029",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-in-memory-pushdown",
            "value": 645776978,
            "range": "± 17823458",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/arrow",
            "value": 557197234,
            "range": "± 25957635",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/parquet",
            "value": 595719113,
            "range": "± 3805032",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-file-compressed",
            "value": 705544137,
            "range": "± 9009380",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-file-uncompressed",
            "value": 666738497,
            "range": "± 10568882",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-in-memory-no-pushdown",
            "value": 1122930724,
            "range": "± 72428824",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-in-memory-pushdown",
            "value": 1131362306,
            "range": "± 27071759",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/arrow",
            "value": 1095737587,
            "range": "± 16637926",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/parquet",
            "value": 1293992594,
            "range": "± 26185152",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-file-compressed",
            "value": 1284593139,
            "range": "± 16273107",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-file-uncompressed",
            "value": 1203145920,
            "range": "± 20526114",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-in-memory-no-pushdown",
            "value": 171727922,
            "range": "± 256594",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-in-memory-pushdown",
            "value": 455861033,
            "range": "± 3911287",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/arrow",
            "value": 158637916,
            "range": "± 2258597",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/parquet",
            "value": 480758153,
            "range": "± 1610856",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-file-compressed",
            "value": 1126535965,
            "range": "± 6089952",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-file-uncompressed",
            "value": 673816446,
            "range": "± 3152590",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-in-memory-no-pushdown",
            "value": 290880123,
            "range": "± 5480415",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-in-memory-pushdown",
            "value": 297186303,
            "range": "± 4892416",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/arrow",
            "value": 286255127,
            "range": "± 4869834",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/parquet",
            "value": 384967536,
            "range": "± 6111838",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-file-compressed",
            "value": 402189263,
            "range": "± 6681295",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-file-uncompressed",
            "value": 371097448,
            "range": "± 4951068",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-in-memory-no-pushdown",
            "value": 945042967,
            "range": "± 8628580",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-in-memory-pushdown",
            "value": 961993237,
            "range": "± 10067796",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/arrow",
            "value": 929912552,
            "range": "± 10525018",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/parquet",
            "value": 1117962970,
            "range": "± 21595589",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-file-compressed",
            "value": 1243825077,
            "range": "± 16489482",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-file-uncompressed",
            "value": 1108045681,
            "range": "± 14206256",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-in-memory-no-pushdown",
            "value": 98336091,
            "range": "± 949562",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-in-memory-pushdown",
            "value": 99008546,
            "range": "± 1754618",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/arrow",
            "value": 67938060,
            "range": "± 1319561",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/parquet",
            "value": 96672223,
            "range": "± 791700",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-file-compressed",
            "value": 113329157,
            "range": "± 829564",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-file-uncompressed",
            "value": 113750435,
            "range": "± 1156542",
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
          "id": "79a22c385ada9b28aebcf4bb050b856472816e88",
          "message": "Mismatched row counting respects query list (#622)",
          "timestamp": "2024-08-14T13:50:27Z",
          "tree_id": "40900641d5370509e659f3ba5baf43c310970a2f",
          "url": "https://github.com/spiraldb/vortex/commit/79a22c385ada9b28aebcf4bb050b856472816e88"
        },
        "date": 1723645220288,
        "tool": "cargo",
        "benches": [
          {
            "name": "tpch_q1/vortex-in-memory-no-pushdown",
            "value": 479009724,
            "range": "± 1682430",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-in-memory-pushdown",
            "value": 480882310,
            "range": "± 2159662",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/arrow",
            "value": 467056533,
            "range": "± 2233485",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/parquet",
            "value": 670412718,
            "range": "± 4075062",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-file-compressed",
            "value": 769135138,
            "range": "± 1778088",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-file-uncompressed",
            "value": 636889997,
            "range": "± 1020480",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-in-memory-no-pushdown",
            "value": 143342074,
            "range": "± 459044",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-in-memory-pushdown",
            "value": 144114314,
            "range": "± 766584",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/arrow",
            "value": 126496794,
            "range": "± 546877",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/parquet",
            "value": 159533707,
            "range": "± 281109",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-file-compressed",
            "value": 176724549,
            "range": "± 601174",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-file-uncompressed",
            "value": 167658624,
            "range": "± 1001806",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-in-memory-no-pushdown",
            "value": 160225105,
            "range": "± 211600",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-in-memory-pushdown",
            "value": 159006319,
            "range": "± 294782",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/arrow",
            "value": 155133927,
            "range": "± 214818",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/parquet",
            "value": 340205939,
            "range": "± 967839",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-file-compressed",
            "value": 270206954,
            "range": "± 1170198",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-file-uncompressed",
            "value": 224269060,
            "range": "± 2093703",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-in-memory-no-pushdown",
            "value": 122214445,
            "range": "± 152016",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-in-memory-pushdown",
            "value": 122131865,
            "range": "± 565136",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/arrow",
            "value": 117949011,
            "range": "± 350751",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/parquet",
            "value": 227298347,
            "range": "± 633086",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-file-compressed",
            "value": 239852194,
            "range": "± 1952948",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-file-uncompressed",
            "value": 195897200,
            "range": "± 2694347",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-in-memory-no-pushdown",
            "value": 301546350,
            "range": "± 726700",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-in-memory-pushdown",
            "value": 300967251,
            "range": "± 1134559",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/arrow",
            "value": 295027981,
            "range": "± 1544358",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/parquet",
            "value": 444554826,
            "range": "± 1520302",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-file-compressed",
            "value": 388099704,
            "range": "± 3742705",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-file-uncompressed",
            "value": 344245752,
            "range": "± 2857895",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-in-memory-no-pushdown",
            "value": 39274756,
            "range": "± 826011",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-in-memory-pushdown",
            "value": 199197713,
            "range": "± 3295067",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/arrow",
            "value": 37586268,
            "range": "± 802456",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/parquet",
            "value": 151095698,
            "range": "± 1633834",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-file-compressed",
            "value": 122634156,
            "range": "± 4059637",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-file-uncompressed",
            "value": 104366268,
            "range": "± 1103315",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-in-memory-no-pushdown",
            "value": 649910069,
            "range": "± 15981170",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-in-memory-pushdown",
            "value": 581785073,
            "range": "± 10717772",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/arrow",
            "value": 560703119,
            "range": "± 5406570",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/parquet",
            "value": 734772346,
            "range": "± 12207839",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-file-compressed",
            "value": 695136608,
            "range": "± 5261412",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-file-uncompressed",
            "value": 653400344,
            "range": "± 4378382",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-in-memory-no-pushdown",
            "value": 228239749,
            "range": "± 3198802",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-in-memory-pushdown",
            "value": 228204387,
            "range": "± 1832475",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/arrow",
            "value": 223536241,
            "range": "± 3242270",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/parquet",
            "value": 490335728,
            "range": "± 4272853",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-file-compressed",
            "value": 343172456,
            "range": "± 4327809",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-file-uncompressed",
            "value": 284427275,
            "range": "± 4990042",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-in-memory-no-pushdown",
            "value": 415517536,
            "range": "± 7618271",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-in-memory-pushdown",
            "value": 408780706,
            "range": "± 3297531",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/arrow",
            "value": 412974834,
            "range": "± 5046601",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/parquet",
            "value": 727392322,
            "range": "± 10041661",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-file-compressed",
            "value": 499235209,
            "range": "± 4658034",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-file-uncompressed",
            "value": 493856572,
            "range": "± 8281687",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-in-memory-no-pushdown",
            "value": 237772475,
            "range": "± 466608",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-in-memory-pushdown",
            "value": 251945708,
            "range": "± 8841501",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/arrow",
            "value": 230390277,
            "range": "± 524529",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/parquet",
            "value": 478510502,
            "range": "± 2225285",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-file-compressed",
            "value": 585291423,
            "range": "± 3293807",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-file-uncompressed",
            "value": 450588285,
            "range": "± 12139463",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-in-memory-no-pushdown",
            "value": 219050857,
            "range": "± 2431728",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-in-memory-pushdown",
            "value": 220651222,
            "range": "± 958781",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/arrow",
            "value": 178888520,
            "range": "± 2308223",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/parquet",
            "value": 188715620,
            "range": "± 265519",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-file-compressed",
            "value": 234217858,
            "range": "± 1386301",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-file-uncompressed",
            "value": 230361165,
            "range": "± 1397329",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-in-memory-no-pushdown",
            "value": 181690590,
            "range": "± 266666",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-in-memory-pushdown",
            "value": 220368693,
            "range": "± 4834585",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/arrow",
            "value": 169925824,
            "range": "± 381809",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/parquet",
            "value": 350203579,
            "range": "± 569982",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-file-compressed",
            "value": 430145545,
            "range": "± 4697495",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-file-uncompressed",
            "value": 330649509,
            "range": "± 3330739",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-in-memory-no-pushdown",
            "value": 292034006,
            "range": "± 1678977",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-in-memory-pushdown",
            "value": 294070564,
            "range": "± 3380947",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/arrow",
            "value": 286806920,
            "range": "± 7143223",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/parquet",
            "value": 432473104,
            "range": "± 1845411",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-file-compressed",
            "value": 348592586,
            "range": "± 4435108",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-file-uncompressed",
            "value": 344678722,
            "range": "± 2767559",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-in-memory-no-pushdown",
            "value": 38682027,
            "range": "± 78124",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-in-memory-pushdown",
            "value": 39376847,
            "range": "± 67009",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/arrow",
            "value": 37854000,
            "range": "± 136880",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/parquet",
            "value": 217702950,
            "range": "± 759566",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-file-compressed",
            "value": 124500713,
            "range": "± 3016135",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-file-uncompressed",
            "value": 105051358,
            "range": "± 1517759",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-in-memory-no-pushdown",
            "value": 120984660,
            "range": "± 177336",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-in-memory-pushdown",
            "value": 125537836,
            "range": "± 477601",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/arrow",
            "value": 109256595,
            "range": "± 1826681",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/parquet",
            "value": 125442396,
            "range": "± 212533",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-file-compressed",
            "value": 141489737,
            "range": "± 614460",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-file-uncompressed",
            "value": 137351720,
            "range": "± 552482",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-in-memory-no-pushdown",
            "value": 641576979,
            "range": "± 22919212",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-in-memory-pushdown",
            "value": 611874395,
            "range": "± 24560052",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/arrow",
            "value": 522177256,
            "range": "± 7836690",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/parquet",
            "value": 609010575,
            "range": "± 5693906",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-file-compressed",
            "value": 682240626,
            "range": "± 15413268",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-file-uncompressed",
            "value": 646188295,
            "range": "± 3655566",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-in-memory-no-pushdown",
            "value": 1100184588,
            "range": "± 23270421",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-in-memory-pushdown",
            "value": 1070280368,
            "range": "± 17784622",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/arrow",
            "value": 1047371698,
            "range": "± 49979683",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/parquet",
            "value": 1289413471,
            "range": "± 43664883",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-file-compressed",
            "value": 1357202607,
            "range": "± 12687140",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-file-uncompressed",
            "value": 1261073476,
            "range": "± 21650039",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-in-memory-no-pushdown",
            "value": 174519335,
            "range": "± 753015",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-in-memory-pushdown",
            "value": 440327228,
            "range": "± 3473177",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/arrow",
            "value": 158560200,
            "range": "± 482267",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/parquet",
            "value": 477906468,
            "range": "± 966798",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-file-compressed",
            "value": 1096156313,
            "range": "± 24043130",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-file-uncompressed",
            "value": 689186259,
            "range": "± 1397254",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-in-memory-no-pushdown",
            "value": 312814449,
            "range": "± 1454360",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-in-memory-pushdown",
            "value": 310813724,
            "range": "± 959799",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/arrow",
            "value": 291216892,
            "range": "± 904535",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/parquet",
            "value": 411643903,
            "range": "± 1380456",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-file-compressed",
            "value": 404366978,
            "range": "± 3680414",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-file-uncompressed",
            "value": 383208031,
            "range": "± 2417082",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-in-memory-no-pushdown",
            "value": 1000129301,
            "range": "± 2796856",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-in-memory-pushdown",
            "value": 1027240911,
            "range": "± 3036993",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/arrow",
            "value": 990224947,
            "range": "± 4584940",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/parquet",
            "value": 1177890271,
            "range": "± 4784777",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-file-compressed",
            "value": 1346288958,
            "range": "± 10136867",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-file-uncompressed",
            "value": 1092186227,
            "range": "± 37516124",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-in-memory-no-pushdown",
            "value": 98842817,
            "range": "± 7122353",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-in-memory-pushdown",
            "value": 99929140,
            "range": "± 666996",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/arrow",
            "value": 68390645,
            "range": "± 1155246",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/parquet",
            "value": 95712492,
            "range": "± 2400376",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-file-compressed",
            "value": 114875265,
            "range": "± 350582",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-file-uncompressed",
            "value": 111901846,
            "range": "± 476736",
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
          "id": "9414b7021d8f31aaeff6c35b080b03690f46d2ce",
          "message": "Fix alp null handling (#623)\n\nCloses #621",
          "timestamp": "2024-08-14T14:40:13Z",
          "tree_id": "cfe7c13b516b1590b39c94798615ea5e9a353bf6",
          "url": "https://github.com/spiraldb/vortex/commit/9414b7021d8f31aaeff6c35b080b03690f46d2ce"
        },
        "date": 1723648161574,
        "tool": "cargo",
        "benches": [
          {
            "name": "tpch_q1/vortex-in-memory-no-pushdown",
            "value": 491260925,
            "range": "± 4421309",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-in-memory-pushdown",
            "value": 491075325,
            "range": "± 3565952",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/arrow",
            "value": 480136697,
            "range": "± 3666766",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/parquet",
            "value": 672539301,
            "range": "± 4968822",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-file-compressed",
            "value": 795550391,
            "range": "± 6260842",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-file-uncompressed",
            "value": 654710424,
            "range": "± 2326253",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-in-memory-no-pushdown",
            "value": 154031647,
            "range": "± 1148676",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-in-memory-pushdown",
            "value": 152683384,
            "range": "± 875993",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/arrow",
            "value": 130749752,
            "range": "± 778310",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/parquet",
            "value": 166925392,
            "range": "± 1888337",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-file-compressed",
            "value": 192937348,
            "range": "± 2370322",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-file-uncompressed",
            "value": 178897485,
            "range": "± 723985",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-in-memory-no-pushdown",
            "value": 166718665,
            "range": "± 2011261",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-in-memory-pushdown",
            "value": 169116673,
            "range": "± 2594017",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/arrow",
            "value": 163263665,
            "range": "± 1359840",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/parquet",
            "value": 357692058,
            "range": "± 2320584",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-file-compressed",
            "value": 285356379,
            "range": "± 4211401",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-file-uncompressed",
            "value": 230796305,
            "range": "± 3974809",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-in-memory-no-pushdown",
            "value": 132788034,
            "range": "± 2323276",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-in-memory-pushdown",
            "value": 131057130,
            "range": "± 1481747",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/arrow",
            "value": 126931487,
            "range": "± 5165300",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/parquet",
            "value": 228409238,
            "range": "± 3756857",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-file-compressed",
            "value": 240468148,
            "range": "± 4518198",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-file-uncompressed",
            "value": 198129290,
            "range": "± 5298903",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-in-memory-no-pushdown",
            "value": 306548851,
            "range": "± 3237302",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-in-memory-pushdown",
            "value": 319960864,
            "range": "± 2104255",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/arrow",
            "value": 296991844,
            "range": "± 1678893",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/parquet",
            "value": 445478002,
            "range": "± 7925779",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-file-compressed",
            "value": 387368573,
            "range": "± 6635313",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-file-uncompressed",
            "value": 345372396,
            "range": "± 3070448",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-in-memory-no-pushdown",
            "value": 38763890,
            "range": "± 210871",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-in-memory-pushdown",
            "value": 187321937,
            "range": "± 138210",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/arrow",
            "value": 35551348,
            "range": "± 496646",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/parquet",
            "value": 149308343,
            "range": "± 870504",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-file-compressed",
            "value": 126346975,
            "range": "± 2949563",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-file-uncompressed",
            "value": 94859839,
            "range": "± 1008122",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-in-memory-no-pushdown",
            "value": 563578820,
            "range": "± 10523265",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-in-memory-pushdown",
            "value": 578685664,
            "range": "± 13582366",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/arrow",
            "value": 553574221,
            "range": "± 12589776",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/parquet",
            "value": 707487378,
            "range": "± 4666162",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-file-compressed",
            "value": 691490661,
            "range": "± 11527978",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-file-uncompressed",
            "value": 648500771,
            "range": "± 15344171",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-in-memory-no-pushdown",
            "value": 224019567,
            "range": "± 1704969",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-in-memory-pushdown",
            "value": 224750889,
            "range": "± 1391456",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/arrow",
            "value": 222522485,
            "range": "± 2144415",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/parquet",
            "value": 483721710,
            "range": "± 9987398",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-file-compressed",
            "value": 342097147,
            "range": "± 6670352",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-file-uncompressed",
            "value": 286798382,
            "range": "± 7074538",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-in-memory-no-pushdown",
            "value": 429226555,
            "range": "± 8168451",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-in-memory-pushdown",
            "value": 407115264,
            "range": "± 5874853",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/arrow",
            "value": 425302179,
            "range": "± 13302714",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/parquet",
            "value": 697770711,
            "range": "± 1317325",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-file-compressed",
            "value": 537785424,
            "range": "± 8459919",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-file-uncompressed",
            "value": 476140160,
            "range": "± 7976842",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-in-memory-no-pushdown",
            "value": 242349461,
            "range": "± 3769445",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-in-memory-pushdown",
            "value": 262625489,
            "range": "± 1055438",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/arrow",
            "value": 237914916,
            "range": "± 1576457",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/parquet",
            "value": 483257875,
            "range": "± 4827293",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-file-compressed",
            "value": 602773878,
            "range": "± 2934975",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-file-uncompressed",
            "value": 463891741,
            "range": "± 2568218",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-in-memory-no-pushdown",
            "value": 224094889,
            "range": "± 8246305",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-in-memory-pushdown",
            "value": 225832670,
            "range": "± 1539386",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/arrow",
            "value": 183307943,
            "range": "± 878069",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/parquet",
            "value": 196195419,
            "range": "± 1146154",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-file-compressed",
            "value": 246015160,
            "range": "± 1493619",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-file-uncompressed",
            "value": 238587342,
            "range": "± 4953746",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-in-memory-no-pushdown",
            "value": 182143634,
            "range": "± 593964",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-in-memory-pushdown",
            "value": 221150547,
            "range": "± 752841",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/arrow",
            "value": 172457672,
            "range": "± 939197",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/parquet",
            "value": 358526980,
            "range": "± 1399118",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-file-compressed",
            "value": 439321530,
            "range": "± 2845262",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-file-uncompressed",
            "value": 336116214,
            "range": "± 1461237",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-in-memory-no-pushdown",
            "value": 338969573,
            "range": "± 9983230",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-in-memory-pushdown",
            "value": 331947241,
            "range": "± 9149478",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/arrow",
            "value": 328099447,
            "range": "± 9549743",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/parquet",
            "value": 483905968,
            "range": "± 6417667",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-file-compressed",
            "value": 362920543,
            "range": "± 3199735",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-file-uncompressed",
            "value": 362529344,
            "range": "± 2854011",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-in-memory-no-pushdown",
            "value": 41416198,
            "range": "± 399434",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-in-memory-pushdown",
            "value": 41826960,
            "range": "± 907089",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/arrow",
            "value": 41679665,
            "range": "± 738108",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/parquet",
            "value": 221705662,
            "range": "± 1010507",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-file-compressed",
            "value": 127320386,
            "range": "± 917785",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-file-uncompressed",
            "value": 104173030,
            "range": "± 2731784",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-in-memory-no-pushdown",
            "value": 124152200,
            "range": "± 1185335",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-in-memory-pushdown",
            "value": 131053904,
            "range": "± 734826",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/arrow",
            "value": 111519867,
            "range": "± 1206355",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/parquet",
            "value": 128647193,
            "range": "± 836847",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-file-compressed",
            "value": 148526633,
            "range": "± 1083828",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-file-uncompressed",
            "value": 140108538,
            "range": "± 782605",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-in-memory-no-pushdown",
            "value": 650710930,
            "range": "± 15324205",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-in-memory-pushdown",
            "value": 642583084,
            "range": "± 5856570",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/arrow",
            "value": 563199786,
            "range": "± 12119692",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/parquet",
            "value": 600976913,
            "range": "± 1917029",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-file-compressed",
            "value": 679856475,
            "range": "± 7981164",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-file-uncompressed",
            "value": 646840897,
            "range": "± 4642949",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-in-memory-no-pushdown",
            "value": 1088525932,
            "range": "± 23311297",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-in-memory-pushdown",
            "value": 1098179695,
            "range": "± 21362527",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/arrow",
            "value": 1108246947,
            "range": "± 17478629",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/parquet",
            "value": 1284648651,
            "range": "± 16801871",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-file-compressed",
            "value": 1236172508,
            "range": "± 28594483",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-file-uncompressed",
            "value": 1126185285,
            "range": "± 4560721",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-in-memory-no-pushdown",
            "value": 169026515,
            "range": "± 1602105",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-in-memory-pushdown",
            "value": 434233670,
            "range": "± 2519225",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/arrow",
            "value": 154617171,
            "range": "± 473663",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/parquet",
            "value": 471101804,
            "range": "± 1778356",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-file-compressed",
            "value": 1116071163,
            "range": "± 6943809",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-file-uncompressed",
            "value": 650923818,
            "range": "± 1767276",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-in-memory-no-pushdown",
            "value": 263166901,
            "range": "± 1922731",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-in-memory-pushdown",
            "value": 260257744,
            "range": "± 955342",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/arrow",
            "value": 242863044,
            "range": "± 751514",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/parquet",
            "value": 354499384,
            "range": "± 1327406",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-file-compressed",
            "value": 360536785,
            "range": "± 9706045",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-file-uncompressed",
            "value": 350408211,
            "range": "± 2256655",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-in-memory-no-pushdown",
            "value": 907437601,
            "range": "± 8064687",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-in-memory-pushdown",
            "value": 937621701,
            "range": "± 12255686",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/arrow",
            "value": 900397510,
            "range": "± 2259217",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/parquet",
            "value": 1074684918,
            "range": "± 15288488",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-file-compressed",
            "value": 1196055007,
            "range": "± 14837954",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-file-uncompressed",
            "value": 1096332651,
            "range": "± 23965120",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-in-memory-no-pushdown",
            "value": 98665432,
            "range": "± 1384478",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-in-memory-pushdown",
            "value": 97854176,
            "range": "± 1160622",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/arrow",
            "value": 68083765,
            "range": "± 774919",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/parquet",
            "value": 98077977,
            "range": "± 1213878",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-file-compressed",
            "value": 113900276,
            "range": "± 1556161",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-file-uncompressed",
            "value": 112958706,
            "range": "± 550682",
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
          "id": "1d550cfba38aeb9d652f6883b5fd6790a6f14923",
          "message": "Fix FoRArray decompression with non 0 shift (#625)",
          "timestamp": "2024-08-14T21:54:30-04:00",
          "tree_id": "80c86bcfa7993bdc6e06eee96872668e6b038317",
          "url": "https://github.com/spiraldb/vortex/commit/1d550cfba38aeb9d652f6883b5fd6790a6f14923"
        },
        "date": 1723688590018,
        "tool": "cargo",
        "benches": [
          {
            "name": "tpch_q1/vortex-in-memory-no-pushdown",
            "value": 478951781,
            "range": "± 906939",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-in-memory-pushdown",
            "value": 479800197,
            "range": "± 1369340",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/arrow",
            "value": 466212277,
            "range": "± 2572852",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/parquet",
            "value": 668012266,
            "range": "± 2026334",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-file-compressed",
            "value": 763782905,
            "range": "± 2346556",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-file-uncompressed",
            "value": 630616127,
            "range": "± 1899690",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-in-memory-no-pushdown",
            "value": 144590210,
            "range": "± 472775",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-in-memory-pushdown",
            "value": 143111002,
            "range": "± 304878",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/arrow",
            "value": 126168164,
            "range": "± 229596",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/parquet",
            "value": 159631275,
            "range": "± 425907",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-file-compressed",
            "value": 178692685,
            "range": "± 888214",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-file-uncompressed",
            "value": 167641396,
            "range": "± 938578",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-in-memory-no-pushdown",
            "value": 159689835,
            "range": "± 777899",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-in-memory-pushdown",
            "value": 162956044,
            "range": "± 1730113",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/arrow",
            "value": 159522872,
            "range": "± 3401067",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/parquet",
            "value": 338878956,
            "range": "± 1832474",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-file-compressed",
            "value": 270785421,
            "range": "± 4653161",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-file-uncompressed",
            "value": 219498279,
            "range": "± 4135099",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-in-memory-no-pushdown",
            "value": 122432799,
            "range": "± 1275084",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-in-memory-pushdown",
            "value": 121057516,
            "range": "± 358448",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/arrow",
            "value": 118117718,
            "range": "± 727661",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/parquet",
            "value": 232361375,
            "range": "± 1442548",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-file-compressed",
            "value": 238243060,
            "range": "± 963688",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-file-uncompressed",
            "value": 192928060,
            "range": "± 1586344",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-in-memory-no-pushdown",
            "value": 305438213,
            "range": "± 2901084",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-in-memory-pushdown",
            "value": 301926198,
            "range": "± 1283254",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/arrow",
            "value": 301380375,
            "range": "± 1930868",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/parquet",
            "value": 448155530,
            "range": "± 2754722",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-file-compressed",
            "value": 391754111,
            "range": "± 4117430",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-file-uncompressed",
            "value": 353285286,
            "range": "± 2774179",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-in-memory-no-pushdown",
            "value": 38991862,
            "range": "± 45676",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-in-memory-pushdown",
            "value": 192695196,
            "range": "± 1661659",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/arrow",
            "value": 35998113,
            "range": "± 84540",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/parquet",
            "value": 147091735,
            "range": "± 186374",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-file-compressed",
            "value": 125436988,
            "range": "± 1082444",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-file-uncompressed",
            "value": 97278107,
            "range": "± 774770",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-in-memory-no-pushdown",
            "value": 558615283,
            "range": "± 2124748",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-in-memory-pushdown",
            "value": 564848959,
            "range": "± 4455408",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/arrow",
            "value": 563275070,
            "range": "± 3391924",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/parquet",
            "value": 717299092,
            "range": "± 3696456",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-file-compressed",
            "value": 689326322,
            "range": "± 4525997",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-file-uncompressed",
            "value": 624434736,
            "range": "± 7714318",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-in-memory-no-pushdown",
            "value": 222461839,
            "range": "± 635915",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-in-memory-pushdown",
            "value": 224136834,
            "range": "± 813526",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/arrow",
            "value": 224115063,
            "range": "± 1275522",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/parquet",
            "value": 480093922,
            "range": "± 2038815",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-file-compressed",
            "value": 341305056,
            "range": "± 5522646",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-file-uncompressed",
            "value": 275444458,
            "range": "± 2721489",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-in-memory-no-pushdown",
            "value": 400533542,
            "range": "± 1669681",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-in-memory-pushdown",
            "value": 402940709,
            "range": "± 1120137",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/arrow",
            "value": 399205689,
            "range": "± 1388220",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/parquet",
            "value": 696160809,
            "range": "± 12439674",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-file-compressed",
            "value": 533371531,
            "range": "± 2971021",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-file-uncompressed",
            "value": 475280434,
            "range": "± 7708384",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-in-memory-no-pushdown",
            "value": 235254291,
            "range": "± 766953",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-in-memory-pushdown",
            "value": 251269106,
            "range": "± 931725",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/arrow",
            "value": 230870658,
            "range": "± 572553",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/parquet",
            "value": 474931636,
            "range": "± 1907529",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-file-compressed",
            "value": 579221128,
            "range": "± 1801145",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-file-uncompressed",
            "value": 441449376,
            "range": "± 1517869",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-in-memory-no-pushdown",
            "value": 209924199,
            "range": "± 642608",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-in-memory-pushdown",
            "value": 209893311,
            "range": "± 466578",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/arrow",
            "value": 173644189,
            "range": "± 605971",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/parquet",
            "value": 186691070,
            "range": "± 428534",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-file-compressed",
            "value": 231330268,
            "range": "± 1268367",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-file-uncompressed",
            "value": 225153756,
            "range": "± 18723077",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-in-memory-no-pushdown",
            "value": 183897764,
            "range": "± 1010555",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-in-memory-pushdown",
            "value": 220738771,
            "range": "± 463671",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/arrow",
            "value": 172098331,
            "range": "± 349574",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/parquet",
            "value": 349379930,
            "range": "± 664653",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-file-compressed",
            "value": 437786855,
            "range": "± 1267965",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-file-uncompressed",
            "value": 336248951,
            "range": "± 679564",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-in-memory-no-pushdown",
            "value": 311470251,
            "range": "± 1208127",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-in-memory-pushdown",
            "value": 309458259,
            "range": "± 1570038",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/arrow",
            "value": 304564731,
            "range": "± 2094619",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/parquet",
            "value": 435222716,
            "range": "± 4365218",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-file-compressed",
            "value": 333567735,
            "range": "± 1377017",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-file-uncompressed",
            "value": 334554950,
            "range": "± 17172096",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-in-memory-no-pushdown",
            "value": 38453194,
            "range": "± 158260",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-in-memory-pushdown",
            "value": 39443870,
            "range": "± 288920",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/arrow",
            "value": 38424495,
            "range": "± 285119",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/parquet",
            "value": 217118195,
            "range": "± 1126193",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-file-compressed",
            "value": 127986660,
            "range": "± 530686",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-file-uncompressed",
            "value": 101867343,
            "range": "± 1557359",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-in-memory-no-pushdown",
            "value": 121207448,
            "range": "± 286933",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-in-memory-pushdown",
            "value": 126466483,
            "range": "± 268058",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/arrow",
            "value": 108456744,
            "range": "± 362528",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/parquet",
            "value": 123969376,
            "range": "± 220474",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-file-compressed",
            "value": 143486932,
            "range": "± 643075",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-file-uncompressed",
            "value": 137974564,
            "range": "± 655471",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-in-memory-no-pushdown",
            "value": 590461705,
            "range": "± 7323401",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-in-memory-pushdown",
            "value": 601820645,
            "range": "± 6287991",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/arrow",
            "value": 518593062,
            "range": "± 8265894",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/parquet",
            "value": 584008776,
            "range": "± 2504037",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-file-compressed",
            "value": 693323975,
            "range": "± 7754096",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-file-uncompressed",
            "value": 660478126,
            "range": "± 5403318",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-in-memory-no-pushdown",
            "value": 1049368372,
            "range": "± 5719776",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-in-memory-pushdown",
            "value": 1050809170,
            "range": "± 5109324",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/arrow",
            "value": 1027437254,
            "range": "± 5248830",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/parquet",
            "value": 1225407892,
            "range": "± 8415060",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-file-compressed",
            "value": 1180664705,
            "range": "± 4686751",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-file-uncompressed",
            "value": 1118771530,
            "range": "± 9747353",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-in-memory-no-pushdown",
            "value": 168384941,
            "range": "± 4184569",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-in-memory-pushdown",
            "value": 434474975,
            "range": "± 2687117",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/arrow",
            "value": 153935101,
            "range": "± 6869445",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/parquet",
            "value": 465868466,
            "range": "± 1075706",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-file-compressed",
            "value": 1089497677,
            "range": "± 1381406",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-file-uncompressed",
            "value": 655607064,
            "range": "± 3024516",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-in-memory-no-pushdown",
            "value": 257647088,
            "range": "± 684433",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-in-memory-pushdown",
            "value": 260167249,
            "range": "± 849188",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/arrow",
            "value": 241211581,
            "range": "± 1040860",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/parquet",
            "value": 353634847,
            "range": "± 1251421",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-file-compressed",
            "value": 353410410,
            "range": "± 2162999",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-file-uncompressed",
            "value": 340653054,
            "range": "± 3342370",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-in-memory-no-pushdown",
            "value": 898527463,
            "range": "± 8257673",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-in-memory-pushdown",
            "value": 910150960,
            "range": "± 2309574",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/arrow",
            "value": 889752541,
            "range": "± 4165596",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/parquet",
            "value": 1040502667,
            "range": "± 3594557",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-file-compressed",
            "value": 1178917295,
            "range": "± 8960115",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-file-uncompressed",
            "value": 1069911467,
            "range": "± 5178075",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-in-memory-no-pushdown",
            "value": 96306259,
            "range": "± 168324",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-in-memory-pushdown",
            "value": 96779033,
            "range": "± 317658",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/arrow",
            "value": 68541266,
            "range": "± 561074",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/parquet",
            "value": 99439750,
            "range": "± 1707483",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-file-compressed",
            "value": 110955100,
            "range": "± 632982",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-file-uncompressed",
            "value": 110333373,
            "range": "± 420500",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "29139614+renovate[bot]@users.noreply.github.com",
            "name": "renovate[bot]",
            "username": "renovate[bot]"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "647a6378245ea970a74fd80c6b5106351e78d71d",
          "message": "chore(deps): update rust crate serde_json to v1.0.125 (#627)\n\n[![Mend\nRenovate](https://app.renovatebot.com/images/banner.svg)](https://renovatebot.com)\n\nThis PR contains the following updates:\n\n| Package | Type | Update | Change |\n|---|---|---|---|\n| [serde_json](https://togithub.com/serde-rs/json) |\nworkspace.dependencies | patch | `1.0.124` -> `1.0.125` |\n\n---\n\n### Release Notes\n\n<details>\n<summary>serde-rs/json (serde_json)</summary>\n\n###\n[`v1.0.125`](https://togithub.com/serde-rs/json/releases/tag/1.0.125)\n\n[Compare\nSource](https://togithub.com/serde-rs/json/compare/v1.0.124...1.0.125)\n\n- Speed up \\uXXXX parsing and improve handling of unpaired surrogates\nwhen deserializing to bytes\n([#&#8203;1172](https://togithub.com/serde-rs/json/issues/1172),\n[#&#8203;1175](https://togithub.com/serde-rs/json/issues/1175), thanks\n[@&#8203;purplesyringa](https://togithub.com/purplesyringa))\n\n</details>\n\n---\n\n### Configuration\n\n📅 **Schedule**: Branch creation - At any time (no schedule defined),\nAutomerge - At any time (no schedule defined).\n\n🚦 **Automerge**: Enabled.\n\n♻ **Rebasing**: Whenever PR becomes conflicted, or you tick the\nrebase/retry checkbox.\n\n🔕 **Ignore**: Close this PR and you won't be reminded about this update\nagain.\n\n---\n\n- [ ] <!-- rebase-check -->If you want to rebase/retry this PR, check\nthis box\n\n---\n\nThis PR was generated by [Mend\nRenovate](https://www.mend.io/free-developer-tools/renovate/). View the\n[repository job log](https://developer.mend.io/github/spiraldb/vortex).\n\n<!--renovate-debug:eyJjcmVhdGVkSW5WZXIiOiIzOC4yNi4xIiwidXBkYXRlZEluVmVyIjoiMzguMjYuMSIsInRhcmdldEJyYW5jaCI6ImRldmVsb3AiLCJsYWJlbHMiOltdfQ==-->\n\nCo-authored-by: renovate[bot] <29139614+renovate[bot]@users.noreply.github.com>",
          "timestamp": "2024-08-15T07:10:17Z",
          "tree_id": "43af8cad05f3bcbd03e10859872074854e0598f2",
          "url": "https://github.com/spiraldb/vortex/commit/647a6378245ea970a74fd80c6b5106351e78d71d"
        },
        "date": 1723707793034,
        "tool": "cargo",
        "benches": [
          {
            "name": "tpch_q1/vortex-in-memory-no-pushdown",
            "value": 496093933,
            "range": "± 3659290",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-in-memory-pushdown",
            "value": 500676026,
            "range": "± 2259157",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/arrow",
            "value": 478297304,
            "range": "± 1434211",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/parquet",
            "value": 685025338,
            "range": "± 2639081",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-file-compressed",
            "value": 798694514,
            "range": "± 3124370",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-file-uncompressed",
            "value": 643053847,
            "range": "± 3902950",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-in-memory-no-pushdown",
            "value": 160414281,
            "range": "± 1375903",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-in-memory-pushdown",
            "value": 160487502,
            "range": "± 1261535",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/arrow",
            "value": 136517416,
            "range": "± 1226709",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/parquet",
            "value": 170784072,
            "range": "± 1802451",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-file-compressed",
            "value": 200888826,
            "range": "± 2149176",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-file-uncompressed",
            "value": 190415330,
            "range": "± 1946428",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-in-memory-no-pushdown",
            "value": 169971539,
            "range": "± 1208628",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-in-memory-pushdown",
            "value": 172030454,
            "range": "± 1373208",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/arrow",
            "value": 166452308,
            "range": "± 2195867",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/parquet",
            "value": 361269494,
            "range": "± 3398543",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-file-compressed",
            "value": 286069408,
            "range": "± 6610483",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-file-uncompressed",
            "value": 235920573,
            "range": "± 4213126",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-in-memory-no-pushdown",
            "value": 129114514,
            "range": "± 1165515",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-in-memory-pushdown",
            "value": 130357111,
            "range": "± 1376742",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/arrow",
            "value": 124873000,
            "range": "± 638768",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/parquet",
            "value": 241195766,
            "range": "± 1038358",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-file-compressed",
            "value": 261489088,
            "range": "± 3390079",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-file-uncompressed",
            "value": 210650217,
            "range": "± 2872041",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-in-memory-no-pushdown",
            "value": 341375997,
            "range": "± 4395082",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-in-memory-pushdown",
            "value": 344178579,
            "range": "± 5589827",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/arrow",
            "value": 338796165,
            "range": "± 5046024",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/parquet",
            "value": 518449856,
            "range": "± 10752547",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-file-compressed",
            "value": 451282733,
            "range": "± 12255747",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-file-uncompressed",
            "value": 390777672,
            "range": "± 2921137",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-in-memory-no-pushdown",
            "value": 43402472,
            "range": "± 188837",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-in-memory-pushdown",
            "value": 197261869,
            "range": "± 2923440",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/arrow",
            "value": 38897997,
            "range": "± 125336",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/parquet",
            "value": 152347335,
            "range": "± 600356",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-file-compressed",
            "value": 129349822,
            "range": "± 1036116",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-file-uncompressed",
            "value": 101073774,
            "range": "± 1027931",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-in-memory-no-pushdown",
            "value": 650979499,
            "range": "± 10898706",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-in-memory-pushdown",
            "value": 643619523,
            "range": "± 4685586",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/arrow",
            "value": 633372778,
            "range": "± 6459785",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/parquet",
            "value": 817447786,
            "range": "± 10735735",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-file-compressed",
            "value": 777138216,
            "range": "± 6689820",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-file-uncompressed",
            "value": 729998871,
            "range": "± 8911407",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-in-memory-no-pushdown",
            "value": 238812690,
            "range": "± 1009000",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-in-memory-pushdown",
            "value": 239117544,
            "range": "± 1792252",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/arrow",
            "value": 232513034,
            "range": "± 2263977",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/parquet",
            "value": 507903622,
            "range": "± 4878587",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-file-compressed",
            "value": 363110884,
            "range": "± 6693168",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-file-uncompressed",
            "value": 311039721,
            "range": "± 5132507",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-in-memory-no-pushdown",
            "value": 476667361,
            "range": "± 6900887",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-in-memory-pushdown",
            "value": 482657497,
            "range": "± 9853174",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/arrow",
            "value": 453839715,
            "range": "± 4719194",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/parquet",
            "value": 759842507,
            "range": "± 9864856",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-file-compressed",
            "value": 610222607,
            "range": "± 19221829",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-file-uncompressed",
            "value": 552438403,
            "range": "± 21150391",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-in-memory-no-pushdown",
            "value": 261867193,
            "range": "± 2464075",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-in-memory-pushdown",
            "value": 281771095,
            "range": "± 5927295",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/arrow",
            "value": 259301373,
            "range": "± 1457923",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/parquet",
            "value": 522180109,
            "range": "± 3318813",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-file-compressed",
            "value": 634146494,
            "range": "± 2584267",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-file-uncompressed",
            "value": 461058133,
            "range": "± 10392070",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-in-memory-no-pushdown",
            "value": 263147359,
            "range": "± 2780706",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-in-memory-pushdown",
            "value": 272311255,
            "range": "± 3521642",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/arrow",
            "value": 209383106,
            "range": "± 2486529",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/parquet",
            "value": 229649558,
            "range": "± 2431455",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-file-compressed",
            "value": 290660748,
            "range": "± 5633209",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-file-uncompressed",
            "value": 282013134,
            "range": "± 2016610",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-in-memory-no-pushdown",
            "value": 192528209,
            "range": "± 1050131",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-in-memory-pushdown",
            "value": 231856890,
            "range": "± 1106601",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/arrow",
            "value": 179458519,
            "range": "± 2270566",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/parquet",
            "value": 373706412,
            "range": "± 8733913",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-file-compressed",
            "value": 459916625,
            "range": "± 1284169",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-file-uncompressed",
            "value": 351825025,
            "range": "± 1613084",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-in-memory-no-pushdown",
            "value": 341230204,
            "range": "± 15602832",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-in-memory-pushdown",
            "value": 370157267,
            "range": "± 3336663",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/arrow",
            "value": 366008847,
            "range": "± 2150950",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/parquet",
            "value": 515302560,
            "range": "± 2834745",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-file-compressed",
            "value": 364034940,
            "range": "± 12371685",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-file-uncompressed",
            "value": 380887980,
            "range": "± 5072767",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-in-memory-no-pushdown",
            "value": 45284423,
            "range": "± 468581",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-in-memory-pushdown",
            "value": 45511152,
            "range": "± 366573",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/arrow",
            "value": 45042944,
            "range": "± 595555",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/parquet",
            "value": 234071658,
            "range": "± 1103619",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-file-compressed",
            "value": 135219468,
            "range": "± 978035",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-file-uncompressed",
            "value": 111204013,
            "range": "± 779529",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-in-memory-no-pushdown",
            "value": 132000074,
            "range": "± 390638",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-in-memory-pushdown",
            "value": 136588969,
            "range": "± 1556212",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/arrow",
            "value": 118102425,
            "range": "± 422027",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/parquet",
            "value": 137088944,
            "range": "± 1087786",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-file-compressed",
            "value": 152378718,
            "range": "± 3287146",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-file-uncompressed",
            "value": 147717817,
            "range": "± 797321",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-in-memory-no-pushdown",
            "value": 788836613,
            "range": "± 24235863",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-in-memory-pushdown",
            "value": 655038763,
            "range": "± 21482640",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/arrow",
            "value": 735672523,
            "range": "± 16040716",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/parquet",
            "value": 651984585,
            "range": "± 9151321",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-file-compressed",
            "value": 771747791,
            "range": "± 10887304",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-file-uncompressed",
            "value": 727354496,
            "range": "± 14530245",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-in-memory-no-pushdown",
            "value": 1315623521,
            "range": "± 7241338",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-in-memory-pushdown",
            "value": 1100528216,
            "range": "± 35238599",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/arrow",
            "value": 1290243771,
            "range": "± 47892873",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/parquet",
            "value": 1494761501,
            "range": "± 14015464",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-file-compressed",
            "value": 1443949681,
            "range": "± 11161882",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-file-uncompressed",
            "value": 1355621896,
            "range": "± 16102316",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-in-memory-no-pushdown",
            "value": 177569969,
            "range": "± 840759",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-in-memory-pushdown",
            "value": 454287702,
            "range": "± 2917756",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/arrow",
            "value": 162477939,
            "range": "± 381232",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/parquet",
            "value": 499770739,
            "range": "± 3610987",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-file-compressed",
            "value": 1181567034,
            "range": "± 3468844",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-file-uncompressed",
            "value": 664631336,
            "range": "± 3165919",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-in-memory-no-pushdown",
            "value": 290068570,
            "range": "± 5924477",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-in-memory-pushdown",
            "value": 301087272,
            "range": "± 8677978",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/arrow",
            "value": 270776584,
            "range": "± 4408072",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/parquet",
            "value": 386022959,
            "range": "± 7312319",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-file-compressed",
            "value": 409723821,
            "range": "± 11496136",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-file-uncompressed",
            "value": 394825605,
            "range": "± 6613777",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-in-memory-no-pushdown",
            "value": 933673762,
            "range": "± 9689637",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-in-memory-pushdown",
            "value": 960674107,
            "range": "± 3804835",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/arrow",
            "value": 943099513,
            "range": "± 7383599",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/parquet",
            "value": 1096863003,
            "range": "± 12151178",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-file-compressed",
            "value": 1233616866,
            "range": "± 6731457",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-file-uncompressed",
            "value": 1112817540,
            "range": "± 14467611",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-in-memory-no-pushdown",
            "value": 101842557,
            "range": "± 1384033",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-in-memory-pushdown",
            "value": 98839020,
            "range": "± 808693",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/arrow",
            "value": 67307578,
            "range": "± 206134",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/parquet",
            "value": 99751370,
            "range": "± 1064237",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-file-compressed",
            "value": 115635231,
            "range": "± 1028927",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-file-uncompressed",
            "value": 114108511,
            "range": "± 1475611",
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
          "id": "a61a052e5b4594c4764d600a2f4492af18db3b64",
          "message": "Bitpacking validity is checked first when getting a scalar (#630)\n\nfixes #624",
          "timestamp": "2024-08-15T10:55:52Z",
          "tree_id": "3c3c3dbf8cf1a6bb8420ee2ffa116c86cc521847",
          "url": "https://github.com/spiraldb/vortex/commit/a61a052e5b4594c4764d600a2f4492af18db3b64"
        },
        "date": 1723721194485,
        "tool": "cargo",
        "benches": [
          {
            "name": "tpch_q1/vortex-in-memory-no-pushdown",
            "value": 497983469,
            "range": "± 2277006",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-in-memory-pushdown",
            "value": 498730868,
            "range": "± 2851910",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/arrow",
            "value": 481670035,
            "range": "± 6986607",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/parquet",
            "value": 688513147,
            "range": "± 5998038",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-file-compressed",
            "value": 800557585,
            "range": "± 20725495",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-file-uncompressed",
            "value": 651288548,
            "range": "± 2002523",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-in-memory-no-pushdown",
            "value": 151652801,
            "range": "± 1290930",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-in-memory-pushdown",
            "value": 149397607,
            "range": "± 810330",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/arrow",
            "value": 129647473,
            "range": "± 493194",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/parquet",
            "value": 163762490,
            "range": "± 838120",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-file-compressed",
            "value": 186982593,
            "range": "± 1107940",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-file-uncompressed",
            "value": 176234329,
            "range": "± 515698",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-in-memory-no-pushdown",
            "value": 161539584,
            "range": "± 1387813",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-in-memory-pushdown",
            "value": 159263128,
            "range": "± 950619",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/arrow",
            "value": 156139436,
            "range": "± 619783",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/parquet",
            "value": 341634233,
            "range": "± 1151248",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-file-compressed",
            "value": 271204140,
            "range": "± 2959651",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-file-uncompressed",
            "value": 220675778,
            "range": "± 4591971",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-in-memory-no-pushdown",
            "value": 123971935,
            "range": "± 791519",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-in-memory-pushdown",
            "value": 123862161,
            "range": "± 602914",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/arrow",
            "value": 120015349,
            "range": "± 1440789",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/parquet",
            "value": 229461937,
            "range": "± 5253020",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-file-compressed",
            "value": 245907796,
            "range": "± 6858160",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-file-uncompressed",
            "value": 199562352,
            "range": "± 6134013",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-in-memory-no-pushdown",
            "value": 308661068,
            "range": "± 1392695",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-in-memory-pushdown",
            "value": 312243193,
            "range": "± 2539282",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/arrow",
            "value": 309386492,
            "range": "± 1379172",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/parquet",
            "value": 461482532,
            "range": "± 3509691",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-file-compressed",
            "value": 404362452,
            "range": "± 2525194",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-file-uncompressed",
            "value": 356184675,
            "range": "± 5712458",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-in-memory-no-pushdown",
            "value": 39988297,
            "range": "± 312154",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-in-memory-pushdown",
            "value": 192564607,
            "range": "± 353632",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/arrow",
            "value": 36178383,
            "range": "± 240222",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/parquet",
            "value": 147158617,
            "range": "± 1917301",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-file-compressed",
            "value": 122498725,
            "range": "± 1305537",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-file-uncompressed",
            "value": 96449303,
            "range": "± 1039314",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-in-memory-no-pushdown",
            "value": 575687089,
            "range": "± 2595236",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-in-memory-pushdown",
            "value": 576272283,
            "range": "± 6643787",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/arrow",
            "value": 574651809,
            "range": "± 13210560",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/parquet",
            "value": 724779575,
            "range": "± 16973703",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-file-compressed",
            "value": 703996145,
            "range": "± 3311301",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-file-uncompressed",
            "value": 651926796,
            "range": "± 4639436",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-in-memory-no-pushdown",
            "value": 223505545,
            "range": "± 815639",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-in-memory-pushdown",
            "value": 223644335,
            "range": "± 851282",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/arrow",
            "value": 221777966,
            "range": "± 1197216",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/parquet",
            "value": 485754601,
            "range": "± 3533693",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-file-compressed",
            "value": 347954474,
            "range": "± 2775473",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-file-uncompressed",
            "value": 295066725,
            "range": "± 3770420",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-in-memory-no-pushdown",
            "value": 418836399,
            "range": "± 1093126",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-in-memory-pushdown",
            "value": 419053694,
            "range": "± 2611547",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/arrow",
            "value": 416864906,
            "range": "± 5473164",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/parquet",
            "value": 701410834,
            "range": "± 2678570",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-file-compressed",
            "value": 562103690,
            "range": "± 11735441",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-file-uncompressed",
            "value": 487032833,
            "range": "± 7263105",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-in-memory-no-pushdown",
            "value": 243351328,
            "range": "± 5842721",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-in-memory-pushdown",
            "value": 260554349,
            "range": "± 985511",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/arrow",
            "value": 236937126,
            "range": "± 1300139",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/parquet",
            "value": 487783470,
            "range": "± 1127741",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-file-compressed",
            "value": 599576492,
            "range": "± 2050230",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-file-uncompressed",
            "value": 458257434,
            "range": "± 1286902",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-in-memory-no-pushdown",
            "value": 223974857,
            "range": "± 5033232",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-in-memory-pushdown",
            "value": 220929928,
            "range": "± 1019335",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/arrow",
            "value": 179632258,
            "range": "± 576689",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/parquet",
            "value": 190460940,
            "range": "± 979664",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-file-compressed",
            "value": 241003119,
            "range": "± 1566595",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-file-uncompressed",
            "value": 234674679,
            "range": "± 1442798",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-in-memory-no-pushdown",
            "value": 185721869,
            "range": "± 1964145",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-in-memory-pushdown",
            "value": 221274013,
            "range": "± 653513",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/arrow",
            "value": 173659195,
            "range": "± 2080243",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/parquet",
            "value": 358609348,
            "range": "± 1090348",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-file-compressed",
            "value": 439565531,
            "range": "± 2748772",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-file-uncompressed",
            "value": 338626518,
            "range": "± 3741764",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-in-memory-no-pushdown",
            "value": 307917500,
            "range": "± 3293126",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-in-memory-pushdown",
            "value": 313955240,
            "range": "± 11020770",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/arrow",
            "value": 308698630,
            "range": "± 4362164",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/parquet",
            "value": 454374058,
            "range": "± 2582800",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-file-compressed",
            "value": 352790143,
            "range": "± 2092481",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-file-uncompressed",
            "value": 346809848,
            "range": "± 5173227",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-in-memory-no-pushdown",
            "value": 39232521,
            "range": "± 233605",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-in-memory-pushdown",
            "value": 39627734,
            "range": "± 201364",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/arrow",
            "value": 40360002,
            "range": "± 712029",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/parquet",
            "value": 217825953,
            "range": "± 790994",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-file-compressed",
            "value": 125229161,
            "range": "± 2061884",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-file-uncompressed",
            "value": 102887387,
            "range": "± 2155794",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-in-memory-no-pushdown",
            "value": 123825390,
            "range": "± 381499",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-in-memory-pushdown",
            "value": 127888887,
            "range": "± 344769",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/arrow",
            "value": 110055848,
            "range": "± 1935344",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/parquet",
            "value": 126351739,
            "range": "± 432812",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-file-compressed",
            "value": 144420210,
            "range": "± 395681",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-file-uncompressed",
            "value": 138936503,
            "range": "± 503145",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-in-memory-no-pushdown",
            "value": 602922331,
            "range": "± 5063866",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-in-memory-pushdown",
            "value": 617403326,
            "range": "± 7457076",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/arrow",
            "value": 539371669,
            "range": "± 7676374",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/parquet",
            "value": 586989401,
            "range": "± 2229349",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-file-compressed",
            "value": 682275799,
            "range": "± 4337234",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-file-uncompressed",
            "value": 653142588,
            "range": "± 12885959",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-in-memory-no-pushdown",
            "value": 1087828089,
            "range": "± 23521340",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-in-memory-pushdown",
            "value": 1074489145,
            "range": "± 18273090",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/arrow",
            "value": 1077074275,
            "range": "± 27495824",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/parquet",
            "value": 1255095029,
            "range": "± 15616429",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-file-compressed",
            "value": 1229703469,
            "range": "± 10200152",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-file-uncompressed",
            "value": 1162786439,
            "range": "± 6425529",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-in-memory-no-pushdown",
            "value": 173330366,
            "range": "± 5203083",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-in-memory-pushdown",
            "value": 435592014,
            "range": "± 2598870",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/arrow",
            "value": 159304912,
            "range": "± 1177830",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/parquet",
            "value": 475562691,
            "range": "± 1782354",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-file-compressed",
            "value": 1099847258,
            "range": "± 3309961",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-file-uncompressed",
            "value": 666755915,
            "range": "± 4320843",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-in-memory-no-pushdown",
            "value": 271030148,
            "range": "± 944748",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-in-memory-pushdown",
            "value": 272475817,
            "range": "± 6786727",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/arrow",
            "value": 254284036,
            "range": "± 2651685",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/parquet",
            "value": 363283314,
            "range": "± 2158697",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-file-compressed",
            "value": 373215212,
            "range": "± 1894103",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-file-uncompressed",
            "value": 359308839,
            "range": "± 2585200",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-in-memory-no-pushdown",
            "value": 915522977,
            "range": "± 6021118",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-in-memory-pushdown",
            "value": 928173216,
            "range": "± 2870744",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/arrow",
            "value": 905283179,
            "range": "± 4711717",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/parquet",
            "value": 1067893947,
            "range": "± 6149697",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-file-compressed",
            "value": 1212808530,
            "range": "± 7397223",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-file-uncompressed",
            "value": 1106163223,
            "range": "± 8793423",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-in-memory-no-pushdown",
            "value": 97409941,
            "range": "± 303355",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-in-memory-pushdown",
            "value": 98358092,
            "range": "± 453779",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/arrow",
            "value": 67271132,
            "range": "± 213899",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/parquet",
            "value": 96352486,
            "range": "± 892910",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-file-compressed",
            "value": 112848014,
            "range": "± 359257",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-file-uncompressed",
            "value": 112098902,
            "range": "± 472200",
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
          "id": "5b1ed72a0a9aef6b6b32d4e8b2ce14110cbb5bde",
          "message": "`Exact` support for more expressions  (#628)\n\nStart moving from `TableProviderFilterPushDown::Inexact` support to\r\n`TableProviderFilterPushDown::Exact` to allow for better performance in\r\nsupported expressions.\r\n\r\n---------\r\n\r\nCo-authored-by: Robert Kruszewski <github@robertk.io>",
          "timestamp": "2024-08-15T13:17:58Z",
          "tree_id": "510efb452205db2248e7024e887c77142eda0285",
          "url": "https://github.com/spiraldb/vortex/commit/5b1ed72a0a9aef6b6b32d4e8b2ce14110cbb5bde"
        },
        "date": 1723729726199,
        "tool": "cargo",
        "benches": [
          {
            "name": "tpch_q1/vortex-in-memory-no-pushdown",
            "value": 504241479,
            "range": "± 2899880",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-in-memory-pushdown",
            "value": 501656270,
            "range": "± 2500977",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/arrow",
            "value": 493865757,
            "range": "± 3729956",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/parquet",
            "value": 686138541,
            "range": "± 2770291",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-file-compressed",
            "value": 812905655,
            "range": "± 6090901",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-file-uncompressed",
            "value": 659369714,
            "range": "± 6045072",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-in-memory-no-pushdown",
            "value": 159268554,
            "range": "± 3160127",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-in-memory-pushdown",
            "value": 161497127,
            "range": "± 1856784",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/arrow",
            "value": 134038190,
            "range": "± 535771",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/parquet",
            "value": 171013944,
            "range": "± 1022292",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-file-compressed",
            "value": 196877360,
            "range": "± 1412821",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-file-uncompressed",
            "value": 185231336,
            "range": "± 2098230",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-in-memory-no-pushdown",
            "value": 174425825,
            "range": "± 2857828",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-in-memory-pushdown",
            "value": 174295813,
            "range": "± 1196770",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/arrow",
            "value": 167942095,
            "range": "± 4945061",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/parquet",
            "value": 381191878,
            "range": "± 4480342",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-file-compressed",
            "value": 295042685,
            "range": "± 5648913",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-file-uncompressed",
            "value": 234236950,
            "range": "± 6016328",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-in-memory-no-pushdown",
            "value": 135699910,
            "range": "± 2197223",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-in-memory-pushdown",
            "value": 127522240,
            "range": "± 1508780",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/arrow",
            "value": 129943414,
            "range": "± 1648226",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/parquet",
            "value": 239882499,
            "range": "± 4338409",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-file-compressed",
            "value": 249325469,
            "range": "± 1986318",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-file-uncompressed",
            "value": 202919654,
            "range": "± 2299088",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-in-memory-no-pushdown",
            "value": 329116937,
            "range": "± 6467466",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-in-memory-pushdown",
            "value": 337558468,
            "range": "± 6410220",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/arrow",
            "value": 329738205,
            "range": "± 8006300",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/parquet",
            "value": 483740569,
            "range": "± 3304558",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-file-compressed",
            "value": 422366916,
            "range": "± 6529482",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-file-uncompressed",
            "value": 363944823,
            "range": "± 3367413",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-in-memory-no-pushdown",
            "value": 44000368,
            "range": "± 340387",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-in-memory-pushdown",
            "value": 196301290,
            "range": "± 1252980",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/arrow",
            "value": 38013822,
            "range": "± 152776",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/parquet",
            "value": 148770801,
            "range": "± 1170415",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-file-compressed",
            "value": 180839855,
            "range": "± 2100532",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-file-uncompressed",
            "value": 125314130,
            "range": "± 1594659",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-in-memory-no-pushdown",
            "value": 591685927,
            "range": "± 5696760",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-in-memory-pushdown",
            "value": 601491992,
            "range": "± 13780669",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/arrow",
            "value": 593268157,
            "range": "± 2373766",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/parquet",
            "value": 771561336,
            "range": "± 10693458",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-file-compressed",
            "value": 734389386,
            "range": "± 13034675",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-file-uncompressed",
            "value": 683244570,
            "range": "± 5321987",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-in-memory-no-pushdown",
            "value": 235941826,
            "range": "± 1762748",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-in-memory-pushdown",
            "value": 230699552,
            "range": "± 1872471",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/arrow",
            "value": 231418860,
            "range": "± 2591362",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/parquet",
            "value": 510230326,
            "range": "± 5380447",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-file-compressed",
            "value": 339654715,
            "range": "± 2435363",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-file-uncompressed",
            "value": 296399676,
            "range": "± 8680631",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-in-memory-no-pushdown",
            "value": 455352773,
            "range": "± 8063952",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-in-memory-pushdown",
            "value": 448245670,
            "range": "± 11785565",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/arrow",
            "value": 450616127,
            "range": "± 10308851",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/parquet",
            "value": 722828183,
            "range": "± 3916449",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-file-compressed",
            "value": 587969725,
            "range": "± 7333870",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-file-uncompressed",
            "value": 521435547,
            "range": "± 8207822",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-in-memory-no-pushdown",
            "value": 248860426,
            "range": "± 2787498",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-in-memory-pushdown",
            "value": 273252067,
            "range": "± 2413555",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/arrow",
            "value": 245581236,
            "range": "± 1512565",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/parquet",
            "value": 496350972,
            "range": "± 2101872",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-file-compressed",
            "value": 612497903,
            "range": "± 4071022",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-file-uncompressed",
            "value": 455633140,
            "range": "± 4515900",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-in-memory-no-pushdown",
            "value": 238130943,
            "range": "± 2397030",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-in-memory-pushdown",
            "value": 233516581,
            "range": "± 1790921",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/arrow",
            "value": 185161311,
            "range": "± 962331",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/parquet",
            "value": 196974481,
            "range": "± 1189478",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-file-compressed",
            "value": 251725390,
            "range": "± 3407955",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-file-uncompressed",
            "value": 245546271,
            "range": "± 4340668",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-in-memory-no-pushdown",
            "value": 184702413,
            "range": "± 736030",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-in-memory-pushdown",
            "value": 221498776,
            "range": "± 538632",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/arrow",
            "value": 172488845,
            "range": "± 864378",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/parquet",
            "value": 361642279,
            "range": "± 4024135",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-file-compressed",
            "value": 775900055,
            "range": "± 3889097",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-file-uncompressed",
            "value": 533274581,
            "range": "± 2513919",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-in-memory-no-pushdown",
            "value": 336778667,
            "range": "± 4198469",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-in-memory-pushdown",
            "value": 339384848,
            "range": "± 5049523",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/arrow",
            "value": 340350235,
            "range": "± 7512744",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/parquet",
            "value": 482392869,
            "range": "± 4812009",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-file-compressed",
            "value": 357846172,
            "range": "± 2869169",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-file-uncompressed",
            "value": 356654984,
            "range": "± 2521259",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-in-memory-no-pushdown",
            "value": 40828611,
            "range": "± 369578",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-in-memory-pushdown",
            "value": 42806234,
            "range": "± 927667",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/arrow",
            "value": 41875342,
            "range": "± 372018",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/parquet",
            "value": 222335137,
            "range": "± 1229423",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-file-compressed",
            "value": 127017075,
            "range": "± 744352",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-file-uncompressed",
            "value": 106046318,
            "range": "± 1582376",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-in-memory-no-pushdown",
            "value": 127229617,
            "range": "± 807121",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-in-memory-pushdown",
            "value": 130681109,
            "range": "± 737666",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/arrow",
            "value": 112490316,
            "range": "± 480485",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/parquet",
            "value": 129166234,
            "range": "± 928962",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-file-compressed",
            "value": 152675904,
            "range": "± 977368",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-file-uncompressed",
            "value": 145812020,
            "range": "± 1134426",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-in-memory-no-pushdown",
            "value": 670750924,
            "range": "± 10818426",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-in-memory-pushdown",
            "value": 669119953,
            "range": "± 28287315",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/arrow",
            "value": 577767590,
            "range": "± 29982834",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/parquet",
            "value": 597152707,
            "range": "± 3387889",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-file-compressed",
            "value": 712427320,
            "range": "± 11059955",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-file-uncompressed",
            "value": 668964070,
            "range": "± 6713552",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-in-memory-no-pushdown",
            "value": 1139899149,
            "range": "± 28561558",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-in-memory-pushdown",
            "value": 1185637776,
            "range": "± 25173844",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/arrow",
            "value": 1190190261,
            "range": "± 34251122",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/parquet",
            "value": 1327397868,
            "range": "± 44696498",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-file-compressed",
            "value": 1305069628,
            "range": "± 28077613",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-file-uncompressed",
            "value": 1214915673,
            "range": "± 17844437",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-in-memory-no-pushdown",
            "value": 172570003,
            "range": "± 926610",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-in-memory-pushdown",
            "value": 455533198,
            "range": "± 2613819",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/arrow",
            "value": 156923101,
            "range": "± 279361",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/parquet",
            "value": 480196417,
            "range": "± 2074057",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-file-compressed",
            "value": 1119938923,
            "range": "± 7770077",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-file-uncompressed",
            "value": 658354580,
            "range": "± 4285039",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-in-memory-no-pushdown",
            "value": 280143938,
            "range": "± 2406766",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-in-memory-pushdown",
            "value": 282958323,
            "range": "± 2662170",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/arrow",
            "value": 268652465,
            "range": "± 7816478",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/parquet",
            "value": 385756573,
            "range": "± 6753260",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-file-compressed",
            "value": 390895045,
            "range": "± 4268694",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-file-uncompressed",
            "value": 374500825,
            "range": "± 4007688",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-in-memory-no-pushdown",
            "value": 946264880,
            "range": "± 5545205",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-in-memory-pushdown",
            "value": 981380322,
            "range": "± 15300182",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/arrow",
            "value": 929380002,
            "range": "± 6654590",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/parquet",
            "value": 1101438697,
            "range": "± 7693388",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-file-compressed",
            "value": 1236596354,
            "range": "± 6969090",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-file-uncompressed",
            "value": 1132065958,
            "range": "± 19090944",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-in-memory-no-pushdown",
            "value": 99228603,
            "range": "± 833830",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-in-memory-pushdown",
            "value": 100949266,
            "range": "± 1201129",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/arrow",
            "value": 68110404,
            "range": "± 280620",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/parquet",
            "value": 96887759,
            "range": "± 448093",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-file-compressed",
            "value": 118026205,
            "range": "± 1237336",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-file-uncompressed",
            "value": 114099070,
            "range": "± 809791",
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
          "id": "5b1ed72a0a9aef6b6b32d4e8b2ce14110cbb5bde",
          "message": "`Exact` support for more expressions  (#628)\n\nStart moving from `TableProviderFilterPushDown::Inexact` support to\r\n`TableProviderFilterPushDown::Exact` to allow for better performance in\r\nsupported expressions.\r\n\r\n---------\r\n\r\nCo-authored-by: Robert Kruszewski <github@robertk.io>",
          "timestamp": "2024-08-15T13:17:58Z",
          "tree_id": "510efb452205db2248e7024e887c77142eda0285",
          "url": "https://github.com/spiraldb/vortex/commit/5b1ed72a0a9aef6b6b32d4e8b2ce14110cbb5bde"
        },
        "date": 1723732678103,
        "tool": "cargo",
        "benches": [
          {
            "name": "tpch_q1/vortex-in-memory-no-pushdown",
            "value": 478427461,
            "range": "± 1571314",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-in-memory-pushdown",
            "value": 479961274,
            "range": "± 2399352",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/arrow",
            "value": 467883426,
            "range": "± 2461061",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/parquet",
            "value": 669808059,
            "range": "± 1731930",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-file-compressed",
            "value": 782473007,
            "range": "± 2338392",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-file-uncompressed",
            "value": 632896703,
            "range": "± 1320550",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-in-memory-no-pushdown",
            "value": 147468059,
            "range": "± 848687",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-in-memory-pushdown",
            "value": 146245834,
            "range": "± 1131581",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/arrow",
            "value": 129155589,
            "range": "± 504813",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/parquet",
            "value": 160523647,
            "range": "± 622471",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-file-compressed",
            "value": 183498175,
            "range": "± 1261499",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-file-uncompressed",
            "value": 171585053,
            "range": "± 787445",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-in-memory-no-pushdown",
            "value": 160341205,
            "range": "± 1167670",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-in-memory-pushdown",
            "value": 158190693,
            "range": "± 713125",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/arrow",
            "value": 156276958,
            "range": "± 737916",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/parquet",
            "value": 338518141,
            "range": "± 2529751",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-file-compressed",
            "value": 265247142,
            "range": "± 1438287",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-file-uncompressed",
            "value": 214213168,
            "range": "± 1140217",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-in-memory-no-pushdown",
            "value": 122634859,
            "range": "± 831595",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-in-memory-pushdown",
            "value": 123790539,
            "range": "± 1185489",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/arrow",
            "value": 119817897,
            "range": "± 1182711",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/parquet",
            "value": 229373902,
            "range": "± 1409432",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-file-compressed",
            "value": 239799460,
            "range": "± 2287530",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-file-uncompressed",
            "value": 191667750,
            "range": "± 2253813",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-in-memory-no-pushdown",
            "value": 309035651,
            "range": "± 2268617",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-in-memory-pushdown",
            "value": 314594922,
            "range": "± 2843041",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/arrow",
            "value": 307040345,
            "range": "± 1968082",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/parquet",
            "value": 463238904,
            "range": "± 2177618",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-file-compressed",
            "value": 392216987,
            "range": "± 3307758",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-file-uncompressed",
            "value": 351215473,
            "range": "± 4591377",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-in-memory-no-pushdown",
            "value": 40231091,
            "range": "± 258524",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-in-memory-pushdown",
            "value": 190341197,
            "range": "± 4139658",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/arrow",
            "value": 36551691,
            "range": "± 271906",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/parquet",
            "value": 149450436,
            "range": "± 463000",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-file-compressed",
            "value": 179083157,
            "range": "± 1120094",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-file-uncompressed",
            "value": 123328963,
            "range": "± 1206439",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-in-memory-no-pushdown",
            "value": 563973619,
            "range": "± 4313686",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-in-memory-pushdown",
            "value": 560743466,
            "range": "± 2668977",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/arrow",
            "value": 559338246,
            "range": "± 1774530",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/parquet",
            "value": 719209864,
            "range": "± 5860300",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-file-compressed",
            "value": 685821899,
            "range": "± 4457816",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-file-uncompressed",
            "value": 636490728,
            "range": "± 8580741",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-in-memory-no-pushdown",
            "value": 220094903,
            "range": "± 1052703",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-in-memory-pushdown",
            "value": 221753680,
            "range": "± 1534228",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/arrow",
            "value": 219054346,
            "range": "± 741159",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/parquet",
            "value": 482380261,
            "range": "± 1494546",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-file-compressed",
            "value": 331793467,
            "range": "± 1170133",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-file-uncompressed",
            "value": 280683110,
            "range": "± 7236434",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-in-memory-no-pushdown",
            "value": 415963603,
            "range": "± 2859068",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-in-memory-pushdown",
            "value": 418516148,
            "range": "± 2860493",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/arrow",
            "value": 413734260,
            "range": "± 2542725",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/parquet",
            "value": 698833310,
            "range": "± 3267667",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-file-compressed",
            "value": 543830000,
            "range": "± 2883976",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-file-uncompressed",
            "value": 495072793,
            "range": "± 7074863",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-in-memory-no-pushdown",
            "value": 240499631,
            "range": "± 896446",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-in-memory-pushdown",
            "value": 253598346,
            "range": "± 1748139",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/arrow",
            "value": 235403816,
            "range": "± 677118",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/parquet",
            "value": 484471194,
            "range": "± 954295",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-file-compressed",
            "value": 591664890,
            "range": "± 1988905",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-file-uncompressed",
            "value": 444854130,
            "range": "± 1991850",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-in-memory-no-pushdown",
            "value": 223050980,
            "range": "± 1733528",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-in-memory-pushdown",
            "value": 221918401,
            "range": "± 1126162",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/arrow",
            "value": 179665625,
            "range": "± 618349",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/parquet",
            "value": 190439752,
            "range": "± 1245714",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-file-compressed",
            "value": 238295930,
            "range": "± 1994284",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-file-uncompressed",
            "value": 233741034,
            "range": "± 1436334",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-in-memory-no-pushdown",
            "value": 180742604,
            "range": "± 1047858",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-in-memory-pushdown",
            "value": 218412928,
            "range": "± 425092",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/arrow",
            "value": 169982175,
            "range": "± 316944",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/parquet",
            "value": 354228652,
            "range": "± 629091",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-file-compressed",
            "value": 753713319,
            "range": "± 2550584",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-file-uncompressed",
            "value": 520469545,
            "range": "± 1997195",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-in-memory-no-pushdown",
            "value": 321228545,
            "range": "± 4494317",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-in-memory-pushdown",
            "value": 319590995,
            "range": "± 4904988",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/arrow",
            "value": 318686372,
            "range": "± 3460456",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/parquet",
            "value": 454481441,
            "range": "± 3213712",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-file-compressed",
            "value": 352164888,
            "range": "± 1671335",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-file-uncompressed",
            "value": 345228437,
            "range": "± 1826405",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-in-memory-no-pushdown",
            "value": 38400057,
            "range": "± 543851",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-in-memory-pushdown",
            "value": 39054778,
            "range": "± 187365",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/arrow",
            "value": 39833579,
            "range": "± 283834",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/parquet",
            "value": 219187079,
            "range": "± 984798",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-file-compressed",
            "value": 123653646,
            "range": "± 1846440",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-file-uncompressed",
            "value": 101270576,
            "range": "± 2771751",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-in-memory-no-pushdown",
            "value": 122994786,
            "range": "± 420402",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-in-memory-pushdown",
            "value": 127392015,
            "range": "± 240663",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/arrow",
            "value": 109402416,
            "range": "± 469607",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/parquet",
            "value": 125666371,
            "range": "± 1478723",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-file-compressed",
            "value": 149388179,
            "range": "± 618649",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-file-uncompressed",
            "value": 141352978,
            "range": "± 1940364",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-in-memory-no-pushdown",
            "value": 608129129,
            "range": "± 7257365",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-in-memory-pushdown",
            "value": 604248623,
            "range": "± 7195368",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/arrow",
            "value": 531172479,
            "range": "± 10505588",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/parquet",
            "value": 581844129,
            "range": "± 4008240",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-file-compressed",
            "value": 668977049,
            "range": "± 3996816",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-file-uncompressed",
            "value": 643659055,
            "range": "± 7489977",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-in-memory-no-pushdown",
            "value": 1067813880,
            "range": "± 8866329",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-in-memory-pushdown",
            "value": 1081890390,
            "range": "± 5472025",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/arrow",
            "value": 1071874831,
            "range": "± 6638706",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/parquet",
            "value": 1264104811,
            "range": "± 8235814",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-file-compressed",
            "value": 1226945542,
            "range": "± 13518846",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-file-uncompressed",
            "value": 1175639481,
            "range": "± 6433576",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-in-memory-no-pushdown",
            "value": 169609542,
            "range": "± 339183",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-in-memory-pushdown",
            "value": 436954336,
            "range": "± 1715266",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/arrow",
            "value": 155675072,
            "range": "± 184667",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/parquet",
            "value": 469894131,
            "range": "± 1235593",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-file-compressed",
            "value": 1090810867,
            "range": "± 3368000",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-file-uncompressed",
            "value": 649268274,
            "range": "± 1454515",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-in-memory-no-pushdown",
            "value": 261192504,
            "range": "± 2496568",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-in-memory-pushdown",
            "value": 266932057,
            "range": "± 3493954",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/arrow",
            "value": 251436770,
            "range": "± 1897629",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/parquet",
            "value": 361694977,
            "range": "± 1218162",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-file-compressed",
            "value": 364167719,
            "range": "± 3881359",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-file-uncompressed",
            "value": 349444396,
            "range": "± 3839442",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-in-memory-no-pushdown",
            "value": 910910254,
            "range": "± 4476941",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-in-memory-pushdown",
            "value": 929728369,
            "range": "± 2703934",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/arrow",
            "value": 908015049,
            "range": "± 5930260",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/parquet",
            "value": 1063048463,
            "range": "± 3573385",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-file-compressed",
            "value": 1186821684,
            "range": "± 5566377",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-file-uncompressed",
            "value": 1071837543,
            "range": "± 8997471",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-in-memory-no-pushdown",
            "value": 97545701,
            "range": "± 242042",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-in-memory-pushdown",
            "value": 98193676,
            "range": "± 293824",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/arrow",
            "value": 67748145,
            "range": "± 221586",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/parquet",
            "value": 95973525,
            "range": "± 559797",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-file-compressed",
            "value": 113688046,
            "range": "± 984379",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-file-uncompressed",
            "value": 111410603,
            "range": "± 632578",
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
          "id": "89c9e0f788b243481a15e8a9cd261e0fd683f84a",
          "message": "Unify expression evaluation for both Table Providers (#632)\n\ncloses #631",
          "timestamp": "2024-08-15T16:20:44+01:00",
          "tree_id": "1c14ce39770972fc0f0204cd5fd94f7161e189c4",
          "url": "https://github.com/spiraldb/vortex/commit/89c9e0f788b243481a15e8a9cd261e0fd683f84a"
        },
        "date": 1723737088174,
        "tool": "cargo",
        "benches": [
          {
            "name": "tpch_q1/vortex-in-memory-no-pushdown",
            "value": 480176320,
            "range": "± 904454",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-in-memory-pushdown",
            "value": 479512552,
            "range": "± 994885",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/arrow",
            "value": 467735484,
            "range": "± 1518397",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/parquet",
            "value": 668398128,
            "range": "± 3852469",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-file-compressed",
            "value": 774299674,
            "range": "± 2454564",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-file-uncompressed",
            "value": 632041000,
            "range": "± 1467974",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-in-memory-no-pushdown",
            "value": 146187722,
            "range": "± 251832",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-in-memory-pushdown",
            "value": 144788869,
            "range": "± 948481",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/arrow",
            "value": 127369560,
            "range": "± 235445",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/parquet",
            "value": 161285002,
            "range": "± 537604",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-file-compressed",
            "value": 181636651,
            "range": "± 1233691",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-file-uncompressed",
            "value": 170784276,
            "range": "± 728706",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-in-memory-no-pushdown",
            "value": 159268609,
            "range": "± 571277",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-in-memory-pushdown",
            "value": 159210207,
            "range": "± 318507",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/arrow",
            "value": 154500372,
            "range": "± 212613",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/parquet",
            "value": 338705375,
            "range": "± 831339",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-file-compressed",
            "value": 273583565,
            "range": "± 7315190",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-file-uncompressed",
            "value": 218067191,
            "range": "± 3264165",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-in-memory-no-pushdown",
            "value": 120699896,
            "range": "± 1356732",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-in-memory-pushdown",
            "value": 120482900,
            "range": "± 865146",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/arrow",
            "value": 117169469,
            "range": "± 286714",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/parquet",
            "value": 228892298,
            "range": "± 737098",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-file-compressed",
            "value": 242989996,
            "range": "± 1358713",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-file-uncompressed",
            "value": 194098337,
            "range": "± 1416923",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-in-memory-no-pushdown",
            "value": 301558545,
            "range": "± 1325467",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-in-memory-pushdown",
            "value": 303673114,
            "range": "± 2086957",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/arrow",
            "value": 299096581,
            "range": "± 1508382",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/parquet",
            "value": 450756103,
            "range": "± 2306642",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-file-compressed",
            "value": 392363793,
            "range": "± 4142876",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-file-uncompressed",
            "value": 339610702,
            "range": "± 5484147",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-in-memory-no-pushdown",
            "value": 38901260,
            "range": "± 118219",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-in-memory-pushdown",
            "value": 190431301,
            "range": "± 2384802",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/arrow",
            "value": 36070991,
            "range": "± 217875",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/parquet",
            "value": 146872468,
            "range": "± 315487",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-file-compressed",
            "value": 175335708,
            "range": "± 581665",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-file-uncompressed",
            "value": 120883849,
            "range": "± 1241491",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-in-memory-no-pushdown",
            "value": 563018393,
            "range": "± 27254032",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-in-memory-pushdown",
            "value": 566414108,
            "range": "± 1238175",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/arrow",
            "value": 564093373,
            "range": "± 3264595",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/parquet",
            "value": 728038358,
            "range": "± 4168959",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-file-compressed",
            "value": 701331444,
            "range": "± 4317858",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-file-uncompressed",
            "value": 638628345,
            "range": "± 2415364",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-in-memory-no-pushdown",
            "value": 217466495,
            "range": "± 945789",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-in-memory-pushdown",
            "value": 221223096,
            "range": "± 260060",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/arrow",
            "value": 218667530,
            "range": "± 933637",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/parquet",
            "value": 481996059,
            "range": "± 1033206",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-file-compressed",
            "value": 343870542,
            "range": "± 2470762",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-file-uncompressed",
            "value": 291148371,
            "range": "± 6720250",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-in-memory-no-pushdown",
            "value": 410251306,
            "range": "± 1523326",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-in-memory-pushdown",
            "value": 407939898,
            "range": "± 1475524",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/arrow",
            "value": 407778886,
            "range": "± 7249389",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/parquet",
            "value": 702922636,
            "range": "± 1906845",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-file-compressed",
            "value": 535784812,
            "range": "± 5971423",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-file-uncompressed",
            "value": 487181655,
            "range": "± 17840849",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-in-memory-no-pushdown",
            "value": 237114887,
            "range": "± 464701",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-in-memory-pushdown",
            "value": 253727848,
            "range": "± 610450",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/arrow",
            "value": 233337945,
            "range": "± 555004",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/parquet",
            "value": 484280361,
            "range": "± 948491",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-file-compressed",
            "value": 587636766,
            "range": "± 2197928",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-file-uncompressed",
            "value": 439639852,
            "range": "± 1934517",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-in-memory-no-pushdown",
            "value": 218399459,
            "range": "± 495778",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-in-memory-pushdown",
            "value": 215456038,
            "range": "± 410521",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/arrow",
            "value": 173391942,
            "range": "± 173838",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/parquet",
            "value": 186521077,
            "range": "± 549035",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-file-compressed",
            "value": 234668837,
            "range": "± 1680427",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-file-uncompressed",
            "value": 226445098,
            "range": "± 970714",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-in-memory-no-pushdown",
            "value": 179803452,
            "range": "± 729247",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-in-memory-pushdown",
            "value": 217390417,
            "range": "± 505008",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/arrow",
            "value": 170254289,
            "range": "± 229815",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/parquet",
            "value": 351698420,
            "range": "± 610172",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-file-compressed",
            "value": 757557010,
            "range": "± 1594232",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-file-uncompressed",
            "value": 523733770,
            "range": "± 2005092",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-in-memory-no-pushdown",
            "value": 298439710,
            "range": "± 4668433",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-in-memory-pushdown",
            "value": 292507902,
            "range": "± 3263172",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/arrow",
            "value": 289610518,
            "range": "± 1492990",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/parquet",
            "value": 435910665,
            "range": "± 1722092",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-file-compressed",
            "value": 346177845,
            "range": "± 1309672",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-file-uncompressed",
            "value": 342749174,
            "range": "± 1551507",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-in-memory-no-pushdown",
            "value": 37444766,
            "range": "± 103297",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-in-memory-pushdown",
            "value": 38831976,
            "range": "± 155031",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/arrow",
            "value": 38986519,
            "range": "± 374084",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/parquet",
            "value": 217213622,
            "range": "± 593481",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-file-compressed",
            "value": 125800996,
            "range": "± 277842",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-file-uncompressed",
            "value": 101286276,
            "range": "± 1421689",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-in-memory-no-pushdown",
            "value": 122829805,
            "range": "± 333451",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-in-memory-pushdown",
            "value": 126772494,
            "range": "± 567065",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/arrow",
            "value": 108925425,
            "range": "± 160621",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/parquet",
            "value": 125534643,
            "range": "± 598336",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-file-compressed",
            "value": 149252526,
            "range": "± 505693",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-file-uncompressed",
            "value": 141786008,
            "range": "± 397128",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-in-memory-no-pushdown",
            "value": 597864573,
            "range": "± 3690783",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-in-memory-pushdown",
            "value": 590189545,
            "range": "± 4830817",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/arrow",
            "value": 509943116,
            "range": "± 6900992",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/parquet",
            "value": 583608929,
            "range": "± 2821137",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-file-compressed",
            "value": 683191027,
            "range": "± 5384922",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-file-uncompressed",
            "value": 653701081,
            "range": "± 5836686",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-in-memory-no-pushdown",
            "value": 1063841227,
            "range": "± 7313732",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-in-memory-pushdown",
            "value": 1068817462,
            "range": "± 10797570",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/arrow",
            "value": 1055851467,
            "range": "± 6622932",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/parquet",
            "value": 1255019805,
            "range": "± 6284816",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-file-compressed",
            "value": 1240056176,
            "range": "± 12583207",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-file-uncompressed",
            "value": 1157068202,
            "range": "± 10458995",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-in-memory-no-pushdown",
            "value": 169330454,
            "range": "± 134078",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-in-memory-pushdown",
            "value": 434014229,
            "range": "± 3471278",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/arrow",
            "value": 154585759,
            "range": "± 321912",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/parquet",
            "value": 467533254,
            "range": "± 549304",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-file-compressed",
            "value": 1088036357,
            "range": "± 1481900",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-file-uncompressed",
            "value": 648069371,
            "range": "± 4457330",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-in-memory-no-pushdown",
            "value": 260180730,
            "range": "± 848194",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-in-memory-pushdown",
            "value": 263850085,
            "range": "± 1021129",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/arrow",
            "value": 246638293,
            "range": "± 1481636",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/parquet",
            "value": 360147601,
            "range": "± 1445698",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-file-compressed",
            "value": 364424456,
            "range": "± 1651576",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-file-uncompressed",
            "value": 349796552,
            "range": "± 2201644",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-in-memory-no-pushdown",
            "value": 901382879,
            "range": "± 4925658",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-in-memory-pushdown",
            "value": 924857384,
            "range": "± 2854757",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/arrow",
            "value": 901945825,
            "range": "± 3739756",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/parquet",
            "value": 1055165418,
            "range": "± 5844368",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-file-compressed",
            "value": 1189683785,
            "range": "± 13861639",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-file-uncompressed",
            "value": 1082570906,
            "range": "± 6340444",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-in-memory-no-pushdown",
            "value": 96227101,
            "range": "± 485834",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-in-memory-pushdown",
            "value": 96530303,
            "range": "± 254619",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/arrow",
            "value": 66910603,
            "range": "± 160696",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/parquet",
            "value": 95014039,
            "range": "± 408117",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-file-compressed",
            "value": 110953684,
            "range": "± 570190",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-file-uncompressed",
            "value": 110014021,
            "range": "± 400013",
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
          "id": "eea02647c207489a1a89ce59a248cf15df35c487",
          "message": "Remove dead code after disk and in memory table provider unification (#633)",
          "timestamp": "2024-08-15T15:59:16Z",
          "tree_id": "3e7a3f7731251f38fdb57a280759ec848fd90e55",
          "url": "https://github.com/spiraldb/vortex/commit/eea02647c207489a1a89ce59a248cf15df35c487"
        },
        "date": 1723739344578,
        "tool": "cargo",
        "benches": [
          {
            "name": "tpch_q1/vortex-in-memory-no-pushdown",
            "value": 483333866,
            "range": "± 5308015",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-in-memory-pushdown",
            "value": 484961833,
            "range": "± 1913840",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/arrow",
            "value": 469506963,
            "range": "± 1329936",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/parquet",
            "value": 673975095,
            "range": "± 2931726",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-file-compressed",
            "value": 789412819,
            "range": "± 4143623",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-file-uncompressed",
            "value": 645450954,
            "range": "± 3152578",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-in-memory-no-pushdown",
            "value": 147956830,
            "range": "± 1227764",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-in-memory-pushdown",
            "value": 148988110,
            "range": "± 2518979",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/arrow",
            "value": 131626778,
            "range": "± 1037425",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/parquet",
            "value": 163381255,
            "range": "± 1833699",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-file-compressed",
            "value": 186522868,
            "range": "± 1631594",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-file-uncompressed",
            "value": 174626999,
            "range": "± 3068806",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-in-memory-no-pushdown",
            "value": 160237799,
            "range": "± 1136853",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-in-memory-pushdown",
            "value": 161308687,
            "range": "± 3569964",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/arrow",
            "value": 157035541,
            "range": "± 1379518",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/parquet",
            "value": 345563557,
            "range": "± 6951495",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-file-compressed",
            "value": 272785523,
            "range": "± 2168384",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-file-uncompressed",
            "value": 221567695,
            "range": "± 4138373",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-in-memory-no-pushdown",
            "value": 124987583,
            "range": "± 1605417",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-in-memory-pushdown",
            "value": 125396983,
            "range": "± 1624161",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/arrow",
            "value": 120797023,
            "range": "± 2233468",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/parquet",
            "value": 231770277,
            "range": "± 1589964",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-file-compressed",
            "value": 243242737,
            "range": "± 3226182",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-file-uncompressed",
            "value": 198695408,
            "range": "± 2374806",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-in-memory-no-pushdown",
            "value": 322639843,
            "range": "± 8855021",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-in-memory-pushdown",
            "value": 326383573,
            "range": "± 5899089",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/arrow",
            "value": 312128552,
            "range": "± 2424336",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/parquet",
            "value": 476574392,
            "range": "± 7961646",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-file-compressed",
            "value": 408201546,
            "range": "± 9015016",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-file-uncompressed",
            "value": 367519991,
            "range": "± 4232077",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-in-memory-no-pushdown",
            "value": 40483935,
            "range": "± 696422",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-in-memory-pushdown",
            "value": 196939704,
            "range": "± 1345050",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/arrow",
            "value": 36287493,
            "range": "± 398323",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/parquet",
            "value": 150103594,
            "range": "± 790862",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-file-compressed",
            "value": 182499162,
            "range": "± 1107297",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-file-uncompressed",
            "value": 126103258,
            "range": "± 1672704",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-in-memory-no-pushdown",
            "value": 588783037,
            "range": "± 10658463",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-in-memory-pushdown",
            "value": 574950521,
            "range": "± 2964249",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/arrow",
            "value": 579615030,
            "range": "± 3484047",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/parquet",
            "value": 743556398,
            "range": "± 14676433",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-file-compressed",
            "value": 703442098,
            "range": "± 5805823",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-file-uncompressed",
            "value": 657192396,
            "range": "± 6541726",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-in-memory-no-pushdown",
            "value": 225669155,
            "range": "± 1628673",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-in-memory-pushdown",
            "value": 226128945,
            "range": "± 1390580",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/arrow",
            "value": 223139494,
            "range": "± 2418308",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/parquet",
            "value": 488131649,
            "range": "± 2981105",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-file-compressed",
            "value": 337137643,
            "range": "± 7509943",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-file-uncompressed",
            "value": 290636767,
            "range": "± 6815752",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-in-memory-no-pushdown",
            "value": 432691528,
            "range": "± 6378134",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-in-memory-pushdown",
            "value": 421258847,
            "range": "± 2472824",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/arrow",
            "value": 408446023,
            "range": "± 2866695",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/parquet",
            "value": 705317714,
            "range": "± 2470690",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-file-compressed",
            "value": 551590390,
            "range": "± 9793655",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-file-uncompressed",
            "value": 480515433,
            "range": "± 4551970",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-in-memory-no-pushdown",
            "value": 247816596,
            "range": "± 2813073",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-in-memory-pushdown",
            "value": 262142691,
            "range": "± 3806996",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/arrow",
            "value": 238909465,
            "range": "± 2205208",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/parquet",
            "value": 483860178,
            "range": "± 2113677",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-file-compressed",
            "value": 601800416,
            "range": "± 7682749",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-file-uncompressed",
            "value": 451853383,
            "range": "± 3504462",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-in-memory-no-pushdown",
            "value": 231273642,
            "range": "± 5898483",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-in-memory-pushdown",
            "value": 228874938,
            "range": "± 5231031",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/arrow",
            "value": 182393488,
            "range": "± 2497516",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/parquet",
            "value": 197119717,
            "range": "± 2199904",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-file-compressed",
            "value": 252194652,
            "range": "± 4258645",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-file-uncompressed",
            "value": 244396859,
            "range": "± 5058729",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-in-memory-no-pushdown",
            "value": 183606508,
            "range": "± 1204648",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-in-memory-pushdown",
            "value": 222400556,
            "range": "± 1511310",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/arrow",
            "value": 174375128,
            "range": "± 1604011",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/parquet",
            "value": 358878560,
            "range": "± 3982385",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-file-compressed",
            "value": 776933254,
            "range": "± 2211742",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-file-uncompressed",
            "value": 534851896,
            "range": "± 2961743",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-in-memory-no-pushdown",
            "value": 353853916,
            "range": "± 4717024",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-in-memory-pushdown",
            "value": 346434089,
            "range": "± 4219517",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/arrow",
            "value": 345631340,
            "range": "± 5299394",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/parquet",
            "value": 474644032,
            "range": "± 12852624",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-file-compressed",
            "value": 371038932,
            "range": "± 2929501",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-file-uncompressed",
            "value": 348020648,
            "range": "± 7159642",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-in-memory-no-pushdown",
            "value": 40085562,
            "range": "± 899963",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-in-memory-pushdown",
            "value": 40111578,
            "range": "± 527616",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/arrow",
            "value": 40783783,
            "range": "± 756711",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/parquet",
            "value": 224316053,
            "range": "± 3706773",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-file-compressed",
            "value": 126606262,
            "range": "± 1183206",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-file-uncompressed",
            "value": 106801294,
            "range": "± 658601",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-in-memory-no-pushdown",
            "value": 126107758,
            "range": "± 1432854",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-in-memory-pushdown",
            "value": 131043670,
            "range": "± 497329",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/arrow",
            "value": 111088724,
            "range": "± 778894",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/parquet",
            "value": 128843030,
            "range": "± 1268388",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-file-compressed",
            "value": 152947644,
            "range": "± 744449",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-file-uncompressed",
            "value": 144259212,
            "range": "± 1652253",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-in-memory-no-pushdown",
            "value": 590939494,
            "range": "± 6322627",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-in-memory-pushdown",
            "value": 620131695,
            "range": "± 20959524",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/arrow",
            "value": 548665933,
            "range": "± 12950454",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/parquet",
            "value": 595474128,
            "range": "± 3628235",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-file-compressed",
            "value": 693862973,
            "range": "± 6601100",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-file-uncompressed",
            "value": 654162253,
            "range": "± 10413803",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-in-memory-no-pushdown",
            "value": 1094336485,
            "range": "± 27455233",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-in-memory-pushdown",
            "value": 1131963653,
            "range": "± 21171978",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/arrow",
            "value": 1129007664,
            "range": "± 27844695",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/parquet",
            "value": 1306078259,
            "range": "± 26798555",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-file-compressed",
            "value": 1287580409,
            "range": "± 16108934",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-file-uncompressed",
            "value": 1206369713,
            "range": "± 19735273",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-in-memory-no-pushdown",
            "value": 170974668,
            "range": "± 730257",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-in-memory-pushdown",
            "value": 442069690,
            "range": "± 3319769",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/arrow",
            "value": 156472959,
            "range": "± 552032",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/parquet",
            "value": 474286320,
            "range": "± 4231249",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-file-compressed",
            "value": 1108487398,
            "range": "± 5219155",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-file-uncompressed",
            "value": 662714719,
            "range": "± 4132876",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-in-memory-no-pushdown",
            "value": 286696493,
            "range": "± 8622298",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-in-memory-pushdown",
            "value": 286254915,
            "range": "± 7260208",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/arrow",
            "value": 267171540,
            "range": "± 6278001",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/parquet",
            "value": 372072077,
            "range": "± 5705138",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-file-compressed",
            "value": 370901373,
            "range": "± 7565186",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-file-uncompressed",
            "value": 367111890,
            "range": "± 3235033",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-in-memory-no-pushdown",
            "value": 932178773,
            "range": "± 9643188",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-in-memory-pushdown",
            "value": 967132660,
            "range": "± 13883421",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/arrow",
            "value": 933025661,
            "range": "± 7891210",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/parquet",
            "value": 1111216918,
            "range": "± 9164329",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-file-compressed",
            "value": 1218036472,
            "range": "± 12386284",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-file-uncompressed",
            "value": 1111358913,
            "range": "± 17357362",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-in-memory-no-pushdown",
            "value": 99882154,
            "range": "± 1164643",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-in-memory-pushdown",
            "value": 98683329,
            "range": "± 615224",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/arrow",
            "value": 67523208,
            "range": "± 171528",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/parquet",
            "value": 97121112,
            "range": "± 1429424",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-file-compressed",
            "value": 113580593,
            "range": "± 577773",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-file-uncompressed",
            "value": 112770367,
            "range": "± 1853062",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "29139614+renovate[bot]@users.noreply.github.com",
            "name": "renovate[bot]",
            "username": "renovate[bot]"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "879bced8ead43d5dbbee4b092b2481af651cedf2",
          "message": "chore(deps): update rust crate serde to v1.0.208 (#634)\n\n[![Mend\nRenovate](https://app.renovatebot.com/images/banner.svg)](https://renovatebot.com)\n\nThis PR contains the following updates:\n\n| Package | Type | Update | Change |\n|---|---|---|---|\n| [serde](https://serde.rs)\n([source](https://togithub.com/serde-rs/serde)) | workspace.dependencies\n| patch | `1.0.207` -> `1.0.208` |\n\n---\n\n### Release Notes\n\n<details>\n<summary>serde-rs/serde (serde)</summary>\n\n###\n[`v1.0.208`](https://togithub.com/serde-rs/serde/releases/tag/v1.0.208)\n\n[Compare\nSource](https://togithub.com/serde-rs/serde/compare/v1.0.207...v1.0.208)\n\n- Support serializing and deserializing unit structs in a `flatten`\nfield ([#&#8203;2802](https://togithub.com/serde-rs/serde/issues/2802),\nthanks [@&#8203;jonhoo](https://togithub.com/jonhoo))\n\n</details>\n\n---\n\n### Configuration\n\n📅 **Schedule**: Branch creation - At any time (no schedule defined),\nAutomerge - At any time (no schedule defined).\n\n🚦 **Automerge**: Enabled.\n\n♻ **Rebasing**: Whenever PR becomes conflicted, or you tick the\nrebase/retry checkbox.\n\n🔕 **Ignore**: Close this PR and you won't be reminded about this update\nagain.\n\n---\n\n- [ ] <!-- rebase-check -->If you want to rebase/retry this PR, check\nthis box\n\n---\n\nThis PR was generated by [Mend\nRenovate](https://www.mend.io/free-developer-tools/renovate/). View the\n[repository job log](https://developer.mend.io/github/spiraldb/vortex).\n\n<!--renovate-debug:eyJjcmVhdGVkSW5WZXIiOiIzOC4yNi4xIiwidXBkYXRlZEluVmVyIjoiMzguMjYuMSIsInRhcmdldEJyYW5jaCI6ImRldmVsb3AiLCJsYWJlbHMiOltdfQ==-->\n\nCo-authored-by: renovate[bot] <29139614+renovate[bot]@users.noreply.github.com>",
          "timestamp": "2024-08-15T16:58:40Z",
          "tree_id": "aeb906d61cc4ad85bf74c08a9783b770da7833d9",
          "url": "https://github.com/spiraldb/vortex/commit/879bced8ead43d5dbbee4b092b2481af651cedf2"
        },
        "date": 1723743052680,
        "tool": "cargo",
        "benches": [
          {
            "name": "tpch_q1/vortex-in-memory-no-pushdown",
            "value": 473015275,
            "range": "± 826552",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-in-memory-pushdown",
            "value": 473697136,
            "range": "± 1900052",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/arrow",
            "value": 460289761,
            "range": "± 1312481",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/parquet",
            "value": 667918003,
            "range": "± 1032745",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-file-compressed",
            "value": 762258578,
            "range": "± 2117171",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-file-uncompressed",
            "value": 631474602,
            "range": "± 1978372",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-in-memory-no-pushdown",
            "value": 144823578,
            "range": "± 2111099",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-in-memory-pushdown",
            "value": 143534681,
            "range": "± 460755",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/arrow",
            "value": 127315582,
            "range": "± 278300",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/parquet",
            "value": 161311425,
            "range": "± 573323",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-file-compressed",
            "value": 179597331,
            "range": "± 704177",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-file-uncompressed",
            "value": 170903335,
            "range": "± 1870239",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-in-memory-no-pushdown",
            "value": 158780195,
            "range": "± 360336",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-in-memory-pushdown",
            "value": 159050085,
            "range": "± 314296",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/arrow",
            "value": 156686839,
            "range": "± 350528",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/parquet",
            "value": 340589383,
            "range": "± 1224664",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-file-compressed",
            "value": 267701021,
            "range": "± 3172890",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-file-uncompressed",
            "value": 218665533,
            "range": "± 3998973",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-in-memory-no-pushdown",
            "value": 123716559,
            "range": "± 497042",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-in-memory-pushdown",
            "value": 124446906,
            "range": "± 487359",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/arrow",
            "value": 120329027,
            "range": "± 381517",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/parquet",
            "value": 230635513,
            "range": "± 671394",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-file-compressed",
            "value": 242559017,
            "range": "± 1565800",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-file-uncompressed",
            "value": 198726101,
            "range": "± 1671533",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-in-memory-no-pushdown",
            "value": 307544316,
            "range": "± 1274796",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-in-memory-pushdown",
            "value": 305229813,
            "range": "± 2525390",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/arrow",
            "value": 300373537,
            "range": "± 1192188",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/parquet",
            "value": 449034885,
            "range": "± 3991199",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-file-compressed",
            "value": 393102353,
            "range": "± 1574622",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-file-uncompressed",
            "value": 350622878,
            "range": "± 2541631",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-in-memory-no-pushdown",
            "value": 39238594,
            "range": "± 84362",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-in-memory-pushdown",
            "value": 190610271,
            "range": "± 329333",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/arrow",
            "value": 36024661,
            "range": "± 60993",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/parquet",
            "value": 150153387,
            "range": "± 374871",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-file-compressed",
            "value": 173841669,
            "range": "± 1933339",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-file-uncompressed",
            "value": 120796054,
            "range": "± 2482532",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-in-memory-no-pushdown",
            "value": 560649941,
            "range": "± 3210607",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-in-memory-pushdown",
            "value": 560666331,
            "range": "± 2307064",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/arrow",
            "value": 556022815,
            "range": "± 2813576",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/parquet",
            "value": 720217301,
            "range": "± 2318300",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-file-compressed",
            "value": 688286458,
            "range": "± 4155805",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-file-uncompressed",
            "value": 637034242,
            "range": "± 4202018",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-in-memory-no-pushdown",
            "value": 223881929,
            "range": "± 1134842",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-in-memory-pushdown",
            "value": 225497057,
            "range": "± 1813615",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/arrow",
            "value": 222562440,
            "range": "± 833279",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/parquet",
            "value": 482981522,
            "range": "± 1743934",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-file-compressed",
            "value": 344672951,
            "range": "± 656656",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-file-uncompressed",
            "value": 280745636,
            "range": "± 833925",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-in-memory-no-pushdown",
            "value": 418317713,
            "range": "± 1223738",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-in-memory-pushdown",
            "value": 417070617,
            "range": "± 2633722",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/arrow",
            "value": 402969062,
            "range": "± 2455712",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/parquet",
            "value": 700181356,
            "range": "± 1326992",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-file-compressed",
            "value": 539855456,
            "range": "± 1783043",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-file-uncompressed",
            "value": 478344357,
            "range": "± 7308000",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-in-memory-no-pushdown",
            "value": 237327249,
            "range": "± 722177",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-in-memory-pushdown",
            "value": 253529954,
            "range": "± 539228",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/arrow",
            "value": 231123827,
            "range": "± 325502",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/parquet",
            "value": 481252079,
            "range": "± 1483368",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-file-compressed",
            "value": 576460003,
            "range": "± 3383239",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-file-uncompressed",
            "value": 441914237,
            "range": "± 967852",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-in-memory-no-pushdown",
            "value": 216513621,
            "range": "± 4504139",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-in-memory-pushdown",
            "value": 217287172,
            "range": "± 844163",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/arrow",
            "value": 175755833,
            "range": "± 360047",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/parquet",
            "value": 188272910,
            "range": "± 483636",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-file-compressed",
            "value": 237903003,
            "range": "± 1150465",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-file-uncompressed",
            "value": 227357643,
            "range": "± 614862",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-in-memory-no-pushdown",
            "value": 181190315,
            "range": "± 259842",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-in-memory-pushdown",
            "value": 218489658,
            "range": "± 320387",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/arrow",
            "value": 170798438,
            "range": "± 417401",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/parquet",
            "value": 353253716,
            "range": "± 903324",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-file-compressed",
            "value": 751806213,
            "range": "± 1188963",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-file-uncompressed",
            "value": 523342538,
            "range": "± 810499",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-in-memory-no-pushdown",
            "value": 294899654,
            "range": "± 8891182",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-in-memory-pushdown",
            "value": 303265067,
            "range": "± 8557221",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/arrow",
            "value": 304137680,
            "range": "± 6340630",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/parquet",
            "value": 438361575,
            "range": "± 2166749",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-file-compressed",
            "value": 340794914,
            "range": "± 1623822",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-file-uncompressed",
            "value": 341992365,
            "range": "± 2254022",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-in-memory-no-pushdown",
            "value": 38605997,
            "range": "± 116354",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-in-memory-pushdown",
            "value": 39184096,
            "range": "± 152489",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/arrow",
            "value": 39519212,
            "range": "± 279137",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/parquet",
            "value": 216648949,
            "range": "± 684622",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-file-compressed",
            "value": 124522528,
            "range": "± 1004972",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-file-uncompressed",
            "value": 102882351,
            "range": "± 1563059",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-in-memory-no-pushdown",
            "value": 122808381,
            "range": "± 476373",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-in-memory-pushdown",
            "value": 126895564,
            "range": "± 1535709",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/arrow",
            "value": 108754889,
            "range": "± 162607",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/parquet",
            "value": 124561883,
            "range": "± 508135",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-file-compressed",
            "value": 149545525,
            "range": "± 593712",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-file-uncompressed",
            "value": 142426197,
            "range": "± 1031132",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-in-memory-no-pushdown",
            "value": 588527418,
            "range": "± 4613776",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-in-memory-pushdown",
            "value": 590858094,
            "range": "± 3795041",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/arrow",
            "value": 515928819,
            "range": "± 5203340",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/parquet",
            "value": 582331548,
            "range": "± 1567145",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-file-compressed",
            "value": 683084193,
            "range": "± 4510334",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-file-uncompressed",
            "value": 646562223,
            "range": "± 6483697",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-in-memory-no-pushdown",
            "value": 1057823360,
            "range": "± 6412989",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-in-memory-pushdown",
            "value": 1061342343,
            "range": "± 5569133",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/arrow",
            "value": 1054207161,
            "range": "± 7586285",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/parquet",
            "value": 1233373119,
            "range": "± 4186316",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-file-compressed",
            "value": 1208816146,
            "range": "± 8074239",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-file-uncompressed",
            "value": 1151405780,
            "range": "± 6956622",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-in-memory-no-pushdown",
            "value": 170152412,
            "range": "± 202989",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-in-memory-pushdown",
            "value": 434071021,
            "range": "± 914402",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/arrow",
            "value": 155392523,
            "range": "± 276829",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/parquet",
            "value": 472671504,
            "range": "± 1212408",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-file-compressed",
            "value": 1082021345,
            "range": "± 1571534",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-file-uncompressed",
            "value": 662046320,
            "range": "± 1947452",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-in-memory-no-pushdown",
            "value": 266228339,
            "range": "± 10990216",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-in-memory-pushdown",
            "value": 266465574,
            "range": "± 1205217",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/arrow",
            "value": 248506463,
            "range": "± 740686",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/parquet",
            "value": 361042428,
            "range": "± 1516583",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-file-compressed",
            "value": 367916951,
            "range": "± 1305265",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-file-uncompressed",
            "value": 352116234,
            "range": "± 4972321",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-in-memory-no-pushdown",
            "value": 913896656,
            "range": "± 3405594",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-in-memory-pushdown",
            "value": 931865970,
            "range": "± 1944560",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/arrow",
            "value": 908349706,
            "range": "± 2973027",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/parquet",
            "value": 1065094526,
            "range": "± 6461920",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-file-compressed",
            "value": 1199292290,
            "range": "± 8265593",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-file-uncompressed",
            "value": 1073817747,
            "range": "± 8014682",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-in-memory-no-pushdown",
            "value": 96975388,
            "range": "± 483889",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-in-memory-pushdown",
            "value": 98561456,
            "range": "± 1264212",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/arrow",
            "value": 66890717,
            "range": "± 138787",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/parquet",
            "value": 94304129,
            "range": "± 598511",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-file-compressed",
            "value": 112873660,
            "range": "± 682063",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-file-uncompressed",
            "value": 110057791,
            "range": "± 254089",
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
          "id": "9baeaf44e384e7c6ef6ab73664464a65ff59434e",
          "message": "Generate more structured inputs for fuzzing (#635)\n\nCloses #629. Doesn't introduce any functional changes but will make\r\nfuture extension easier IMO.",
          "timestamp": "2024-08-15T17:12:39Z",
          "tree_id": "f9d02e3d5927e4ce269f569708e67bbd7543b8a0",
          "url": "https://github.com/spiraldb/vortex/commit/9baeaf44e384e7c6ef6ab73664464a65ff59434e"
        },
        "date": 1723743773892,
        "tool": "cargo",
        "benches": [
          {
            "name": "tpch_q1/vortex-in-memory-no-pushdown",
            "value": 484095300,
            "range": "± 2979267",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-in-memory-pushdown",
            "value": 482597725,
            "range": "± 2440338",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/arrow",
            "value": 468255359,
            "range": "± 1870668",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/parquet",
            "value": 684252754,
            "range": "± 2769193",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-file-compressed",
            "value": 785801663,
            "range": "± 3112844",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q1/vortex-file-uncompressed",
            "value": 642672959,
            "range": "± 4308953",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-in-memory-no-pushdown",
            "value": 150167540,
            "range": "± 586827",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-in-memory-pushdown",
            "value": 150311368,
            "range": "± 2510520",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/arrow",
            "value": 130106798,
            "range": "± 476925",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/parquet",
            "value": 165253257,
            "range": "± 1023894",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-file-compressed",
            "value": 186556365,
            "range": "± 741038",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q2/vortex-file-uncompressed",
            "value": 175645220,
            "range": "± 1008389",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-in-memory-no-pushdown",
            "value": 169001746,
            "range": "± 1551351",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-in-memory-pushdown",
            "value": 168756844,
            "range": "± 1383420",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/arrow",
            "value": 163440225,
            "range": "± 1449955",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/parquet",
            "value": 365497789,
            "range": "± 2295011",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-file-compressed",
            "value": 282215167,
            "range": "± 3861357",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q3/vortex-file-uncompressed",
            "value": 229222886,
            "range": "± 3920123",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-in-memory-no-pushdown",
            "value": 134935275,
            "range": "± 1068755",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-in-memory-pushdown",
            "value": 132070037,
            "range": "± 2249211",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/arrow",
            "value": 125912352,
            "range": "± 1179879",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/parquet",
            "value": 244230227,
            "range": "± 1853172",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-file-compressed",
            "value": 256824086,
            "range": "± 2395410",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q4/vortex-file-uncompressed",
            "value": 207314368,
            "range": "± 3158632",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-in-memory-no-pushdown",
            "value": 323164189,
            "range": "± 4876395",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-in-memory-pushdown",
            "value": 322889263,
            "range": "± 2519183",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/arrow",
            "value": 320574210,
            "range": "± 2556032",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/parquet",
            "value": 486717750,
            "range": "± 3926108",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-file-compressed",
            "value": 413287421,
            "range": "± 2498616",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q5/vortex-file-uncompressed",
            "value": 370357634,
            "range": "± 5318694",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-in-memory-no-pushdown",
            "value": 42498033,
            "range": "± 312353",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-in-memory-pushdown",
            "value": 193420311,
            "range": "± 1822778",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/arrow",
            "value": 38080996,
            "range": "± 267872",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/parquet",
            "value": 154186268,
            "range": "± 635991",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-file-compressed",
            "value": 184325187,
            "range": "± 1160587",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q6/vortex-file-uncompressed",
            "value": 128343566,
            "range": "± 1405774",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-in-memory-no-pushdown",
            "value": 603378732,
            "range": "± 5825120",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-in-memory-pushdown",
            "value": 611781370,
            "range": "± 21863195",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/arrow",
            "value": 588444637,
            "range": "± 5970712",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/parquet",
            "value": 762177742,
            "range": "± 5319435",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-file-compressed",
            "value": 720094669,
            "range": "± 5493819",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q7/vortex-file-uncompressed",
            "value": 665889795,
            "range": "± 7839542",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-in-memory-no-pushdown",
            "value": 231968213,
            "range": "± 811598",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-in-memory-pushdown",
            "value": 232309712,
            "range": "± 1441117",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/arrow",
            "value": 228557648,
            "range": "± 3138889",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/parquet",
            "value": 503000411,
            "range": "± 4742305",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-file-compressed",
            "value": 343269964,
            "range": "± 7000707",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q8/vortex-file-uncompressed",
            "value": 284539973,
            "range": "± 2474905",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-in-memory-no-pushdown",
            "value": 437880003,
            "range": "± 2980815",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-in-memory-pushdown",
            "value": 434032488,
            "range": "± 4170978",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/arrow",
            "value": 428287778,
            "range": "± 5058128",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/parquet",
            "value": 720934428,
            "range": "± 3906110",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-file-compressed",
            "value": 571703863,
            "range": "± 7621940",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q9/vortex-file-uncompressed",
            "value": 491177120,
            "range": "± 6528932",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-in-memory-no-pushdown",
            "value": 244530196,
            "range": "± 678899",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-in-memory-pushdown",
            "value": 262641600,
            "range": "± 2119063",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/arrow",
            "value": 241231147,
            "range": "± 1837205",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/parquet",
            "value": 499279404,
            "range": "± 3052627",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-file-compressed",
            "value": 602959937,
            "range": "± 2551629",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q10/vortex-file-uncompressed",
            "value": 461100184,
            "range": "± 2197094",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-in-memory-no-pushdown",
            "value": 239124513,
            "range": "± 2855355",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-in-memory-pushdown",
            "value": 239821523,
            "range": "± 5181846",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/arrow",
            "value": 187039202,
            "range": "± 1298530",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/parquet",
            "value": 199355083,
            "range": "± 2120038",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-file-compressed",
            "value": 243961793,
            "range": "± 1437770",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q11/vortex-file-uncompressed",
            "value": 239203628,
            "range": "± 2586148",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-in-memory-no-pushdown",
            "value": 183799897,
            "range": "± 863530",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-in-memory-pushdown",
            "value": 223019570,
            "range": "± 830570",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/arrow",
            "value": 173579492,
            "range": "± 428131",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/parquet",
            "value": 359988370,
            "range": "± 2589930",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-file-compressed",
            "value": 760621865,
            "range": "± 3273606",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q12/vortex-file-uncompressed",
            "value": 533360321,
            "range": "± 1612040",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-in-memory-no-pushdown",
            "value": 336362891,
            "range": "± 5295368",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-in-memory-pushdown",
            "value": 333115704,
            "range": "± 3775125",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/arrow",
            "value": 334658407,
            "range": "± 3522829",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/parquet",
            "value": 471125475,
            "range": "± 5375078",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-file-compressed",
            "value": 354225343,
            "range": "± 1826430",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q13/vortex-file-uncompressed",
            "value": 353605655,
            "range": "± 1954123",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-in-memory-no-pushdown",
            "value": 40576899,
            "range": "± 382769",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-in-memory-pushdown",
            "value": 41263165,
            "range": "± 400567",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/arrow",
            "value": 41695846,
            "range": "± 444690",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/parquet",
            "value": 225346810,
            "range": "± 1672506",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-file-compressed",
            "value": 128013305,
            "range": "± 830547",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q14/vortex-file-uncompressed",
            "value": 104804850,
            "range": "± 1114940",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-in-memory-no-pushdown",
            "value": 124192944,
            "range": "± 355926",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-in-memory-pushdown",
            "value": 128344689,
            "range": "± 675149",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/arrow",
            "value": 110493950,
            "range": "± 302916",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/parquet",
            "value": 129033755,
            "range": "± 676746",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-file-compressed",
            "value": 151174863,
            "range": "± 700430",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q16/vortex-file-uncompressed",
            "value": 143505037,
            "range": "± 834948",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-in-memory-no-pushdown",
            "value": 650496426,
            "range": "± 12318577",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-in-memory-pushdown",
            "value": 652721634,
            "range": "± 13452834",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/arrow",
            "value": 556721385,
            "range": "± 8689482",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/parquet",
            "value": 600545292,
            "range": "± 3815835",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-file-compressed",
            "value": 703137322,
            "range": "± 4424059",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q17/vortex-file-uncompressed",
            "value": 660107132,
            "range": "± 7101539",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-in-memory-no-pushdown",
            "value": 1111762515,
            "range": "± 14146490",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-in-memory-pushdown",
            "value": 1118246099,
            "range": "± 12794602",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/arrow",
            "value": 1115609416,
            "range": "± 8775340",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/parquet",
            "value": 1291679138,
            "range": "± 9127602",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-file-compressed",
            "value": 1271822712,
            "range": "± 17899860",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q18/vortex-file-uncompressed",
            "value": 1201185522,
            "range": "± 9114122",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-in-memory-no-pushdown",
            "value": 172193516,
            "range": "± 298290",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-in-memory-pushdown",
            "value": 432842230,
            "range": "± 1747725",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/arrow",
            "value": 157956324,
            "range": "± 375480",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/parquet",
            "value": 480850196,
            "range": "± 1088183",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-file-compressed",
            "value": 1093730170,
            "range": "± 3769647",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q19/vortex-file-uncompressed",
            "value": 665161830,
            "range": "± 5170210",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-in-memory-no-pushdown",
            "value": 283515809,
            "range": "± 3730466",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-in-memory-pushdown",
            "value": 280538777,
            "range": "± 2731416",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/arrow",
            "value": 262593690,
            "range": "± 2098739",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/parquet",
            "value": 381473589,
            "range": "± 4064846",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-file-compressed",
            "value": 381397684,
            "range": "± 6177346",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q20/vortex-file-uncompressed",
            "value": 362973057,
            "range": "± 4688282",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-in-memory-no-pushdown",
            "value": 955784001,
            "range": "± 9275726",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-in-memory-pushdown",
            "value": 985274144,
            "range": "± 4806623",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/arrow",
            "value": 947442255,
            "range": "± 5349832",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/parquet",
            "value": 1122884300,
            "range": "± 13478460",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-file-compressed",
            "value": 1240913047,
            "range": "± 13350948",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q21/vortex-file-uncompressed",
            "value": 1128545040,
            "range": "± 13818499",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-in-memory-no-pushdown",
            "value": 100901804,
            "range": "± 625959",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-in-memory-pushdown",
            "value": 100710106,
            "range": "± 811779",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/arrow",
            "value": 69188407,
            "range": "± 832667",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/parquet",
            "value": 96607438,
            "range": "± 746150",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-file-compressed",
            "value": 115192911,
            "range": "± 636322",
            "unit": "ns/iter"
          },
          {
            "name": "tpch_q22/vortex-file-uncompressed",
            "value": 114418275,
            "range": "± 655395",
            "unit": "ns/iter"
          }
        ]
      }
    ]
  }
}