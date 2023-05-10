/**
 * Copyright (c) Malkov, Yu A., and D. A. Yashunin.
 *
 * This hnswlib source code is licensed under the Apache-2.0 License.
 * https://github.com/nmslib/hnswlib/blob/master/LICENSE
 *
 *
 * The works below are modified based on hnswlib:
 * 1. Replace the static batch indexing with real time indexing
 * 2. Add the numeric field and bitmap filters in the process of searching
 *
 * Modified works copyright 2020 The Gamma Authors.
 *
 * The modified codes are licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 *
 */

#pragma once

#include <mutex>
#include <string.h>
#include <deque>

namespace hnswlib {
    typedef unsigned short int vl_type;

    class VisitedList {
    public:
        vl_type curV;
        vl_type *mass;
        unsigned int numelements;

        VisitedList(int numelements1) {
            curV = -1;
            numelements = numelements1;
            mass = new vl_type[numelements];
        }

        void reset() {
            curV++;
            if (curV == 0) {
                memset(mass, 0, sizeof(vl_type) * numelements);
                curV++;
            }
        };

        ~VisitedList() { delete[] mass; }
    };
///////////////////////////////////////////////////////////
//
// Class for multi-threaded pool-management of VisitedLists
//
/////////////////////////////////////////////////////////

    class VisitedListPool {
        std::deque<VisitedList *> pool;
        std::mutex poolguard;
        int numelements;

    public:
        VisitedListPool(int initmaxpools, int numelements1) {
            numelements = numelements1;
            for (int i = 0; i < initmaxpools; i++)
                pool.push_front(new VisitedList(numelements));
        }

        VisitedList *getFreeVisitedList() {
            VisitedList *rez;
            {
                std::unique_lock <std::mutex> lock(poolguard);
                if (pool.size() > 0) {
                    rez = pool.front();
                    pool.pop_front();
                } else {
                    rez = new VisitedList(numelements);
                }
            }
            rez->reset();
            return rez;
        };

        void releaseVisitedList(VisitedList *vl) {
            std::unique_lock <std::mutex> lock(poolguard);
            pool.push_front(vl);
        };

        ~VisitedListPool() {
            while (pool.size()) {
                VisitedList *rez = pool.front();
                pool.pop_front();
                delete rez;
            }
        };
    };
}

