# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Define schema for TPC-H tables
DDS_DDL = {
    'NATION': [
        ('N_NATIONKEY', 'int8', False, 'none', 'auto', None, None, 0),
        ('N_REGIONKEY', 'int8', False, 'none', 'auto', None, None, 2),
        ('N_NAME', 'string', False, 'none', 'auto', None, None, 1),
        ('N_COMMENT', 'string', True, 'none', 'auto', None, None, 3),
    ],
    'REGION': [
        ('R_REGIONKEY', 'int8', False, 'none', 'auto', None, None, 0),
        ('R_NAME', 'string', False, 'none', 'auto', None, None, 1),
        ('R_COMMENT', 'string', True, 'none', 'auto', None, None, 2),
    ],
    'PART': [
        ('P_PARTKEY', 'int64', False, 'none', 'auto', None, None, 0),
        ('P_NAME', 'string', False, 'none', 'auto', None, None, 1),
        ('P_MFGR', 'string', False, 'none', 'auto', None, None, 2),
        ('P_BRAND', 'string', False, 'none', 'auto', None, None, 3),
        ('P_TYPE', 'string', False, 'none', 'auto', None, None, 4),
        ('P_SIZE', 'int8', False, 'none', 'auto', None, None, 5),
        ('P_CONTAINER', 'string', False, 'none', 'auto', None, None, 6),
        ('P_RETAILPRICE', 'float', False, 'none', 'auto', None, None, 7),
        ('P_COMMENT', 'string', False, 'none', 'auto', None, None, 8),
    ],
    'SUPPLIER': [
        ('S_SUPPKEY', 'int64', False, 'none', 'auto', None, None, 0),
        ('S_NATIONKEY', 'int8', False, 'none', 'auto', None, None, 3),
        ('S_NAME', 'string', False, 'none', 'auto', None, None, 1),
        ('S_ADDRESS', 'string', False, 'none', 'auto', None, None, 2),
        ('S_PHONE', 'string', False, 'none', 'auto', None, None, 4),
        ('S_ACCTBAL', 'float', False, 'none', 'auto', None, None, 5),
        ('S_COMMENT', 'string', False, 'none', 'auto', None, None, 6),
    ],
    'PARTSUPP': [
        ('PS_PARTKEY', 'int64', False, 'none', 'auto', None, None, 0),
        ('PS_SUPPKEY', 'int64', False, 'none', 'auto', None, None, 1),
        ('PS_AVAILQTY', 'int16', False, 'none', 'auto', None, None, 2),
        ('PS_SUPPLYCOST', 'float', False, 'none', 'auto', None, None, 3),
        ('PS_COMMENT', 'string', False, 'none', 'auto', None, None, 4),
    ],
    'CUSTOMER': [
        ('C_CUSTKEY', 'int64', False, 'none', 'auto', None, None, 0),
        ('C_NATIONKEY', 'int8', False, 'none', 'auto', None, None, 3),
        ('C_NAME', 'string', False, 'none', 'auto', None, None, 1),
        ('C_ADDRESS', 'string', False, 'none', 'auto', None, None, 2),
        ('C_PHONE', 'string', False, 'none', 'auto', None, None, 4),
        ('C_ACCTBAL', 'float', False, 'none', 'auto', None, None, 5),
        ('C_MKTSEGMENT', 'string', False, 'none', 'auto', None, None, 6),
        ('C_COMMENT', 'string', False, 'none', 'auto', None, None, 7),
    ],
    'ORDERS': [
        ('O_ORDERKEY', 'int64', False, 'none', 'auto', None, None, 0),
        ('O_CUSTKEY', 'int64', False, 'none', 'auto', None, None, 1),
        ('O_ORDERSTATUS', 'string', False, 'none', 'auto', None, None, 2),
        ('O_TOTALPRICE', 'float', False, 'none', 'auto', None, None, 3),
        ('O_ORDERDATE', 'unixtime_micros', False, 'none', 'auto',
         None, None, 4),
        ('O_ORDERPRIORITY', 'string', False, 'none', 'auto', None, None, 5),
        ('O_CLERK', 'string', False, 'none', 'auto', None, None, 6),
        ('O_SHIPPRIORITY', 'int8', False, 'none', 'auto', None, None, 7),
        ('O_COMMENT', 'string', False, 'none', 'auto', None, None, 8),
    ],
    'LINEITEM': [
        ('L_ORDERKEY', 'int64', False, 'none', 'auto', None, None, 0),
        ('L_PARTKEY', 'int64', False, 'none', 'auto', None, None, 1),
        ('L_SUPPKEY', 'int64', False, 'none', 'auto', None, None, 2),
        ('L_LINENUMBER', 'int8', False, 'none', 'auto', None, None, 3),
        ('L_QUANTITY', 'float', False, 'none', 'auto', None, None, 4),
        ('L_EXTENDEDPRICE', 'float', False, 'none', 'auto', None, None, 5),
        ('L_DISCOUNT', 'float', False, 'none', 'auto', None, None, 6),
        ('L_TAX', 'float', False, 'none', 'auto', None, None, 7),
        ('L_RETURNFLAG', 'string', False, 'none', 'auto', None, None, 8),
        ('L_LINESTATUS', 'string', False, 'none', 'auto', None, None, 9),
        ('L_SHIPDATE', 'unixtime_micros', False, 'none', 'auto',
         None, None, 10),
        ('L_COMMITDATE', 'unixtime_micros', False, 'none', 'auto',
         None, None, 11),
        ('L_RECEIPTDATE', 'unixtime_micros', False, 'none', 'auto',
         None, None, 12),
        ('L_SHIPINSTRUCT', 'string', False, 'none', 'auto', None, None, 13),
        ('L_SHIPMODE', 'string', False, 'none', 'auto', None, None, 14),
        ('L_COMMENT', 'string', False, 'none', 'auto', None, None, 15),
    ],
}

# Define hash partitions for TPC-H tables
DDS_RI = {
    'REGION': {
        'REGION_PK': [
            'R_REGIONKEY',
        ]
    },
    'NATION': {
        'NATION_PK': [
            'N_NATIONKEY',
        ],
        'NATION_FK1': [
            'N_REGIONKEY',
        ],
    },
    'PART': {
        'PART_PK': [
            'P_PARTKEY',
        ],
    },
    'SUPPLIER': {
        'SUPPLIER_PK': [
            'S_SUPPKEY',
        ],
        'SUPPLIER_FK1': [
            'S_NATIONKEY',
        ],
    },
    'PARTSUPP': {
        # skip hash bucket schema components that contain columns in common
        #'PARTSUPP_PK': [
        #    'PS_PARTKEY',
        #    'PS_SUPPKEY',
        #],
        'PARTSUPP_FK1': [
            'PS_SUPPKEY',
        ],
        'PARTSUPP_FK2': [
            'PS_PARTKEY',
        ],
    },
    'CUSTOMER': {
        'CUSTOMER_PK': [
            'C_CUSTKEY',
        ],
        'CUSTOMER_FK1': [
            'C_NATIONKEY',
        ],
    },
    'LINEITEM': {
        #'LINEITEM_PK': [
        #    'L_ORDERKEY',
        #    'L_LINENUMBER',
        #],
        'LINEITEM_FK1': [
            'L_ORDERKEY',
        ],
        #'LINEITEM_FK2': [
        #    'L_PARTKEY',
        #    'L_SUPPKEY',
        #],
        'CUSTOM_FK1': [
            'L_LINENUMBER',
        ],
        'CUSTOM_FK2': [
            'L_PARTKEY',
        ],
        'CUSTOM_FK3': [
            'L_SUPPKEY',
        ],
    },
    'ORDERS': {
        'ORDERS_PK': [
            'O_ORDERKEY',
        ],
        'ORDERS_FK1': [
            'O_CUSTKEY',
        ],
    },
}

# Define bucket numbers for hash partitions
NUM_BUCKETS = {
    'REGION_PK': 5,
    'NATION_PK': 7,
    'NATION_FK1': 5,
    'PART_PK': 67,
    'SUPPLIER_PK': 13,
    'SUPPLIER_FK1': 5,
    'PARTSUPP_PK': None,
    'PARTSUPP_FK1': 2,
    'PARTSUPP_FK2': 37,
    'CUSTOMER_PK': 13,
    'CUSTOMER_FK1': 5,
    'LINEITEM_PK': None,
    'LINEITEM_FK1': 7,
    'LINEITEM_FK2': None,
    'CUSTOM_FK1': 2,
    'CUSTOM_FK2': 3,
    'CUSTOM_FK3': 2,
    'ORDERS_PK': 43,
    'ORDERS_FK1': 2,
}
