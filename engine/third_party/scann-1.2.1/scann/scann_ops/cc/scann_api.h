/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#ifndef SCANN_API_H_
#define SCANN_API_H_



void *ScannInit(const char *config_str, int len);

void ScannClose(void *scann);

int ScannTraining(void *scann, const char *dataset, int len, int dim,
                  int training_threads);

int ScannAddIndex(void *scann, const char *dataset, int len);

int ScannSearch(void *scann, const float *queries, int points_num,
                int final_nn, int pre_reorder_nn, int leaves,
                std::vector<std::vector<std::pair<uint32_t, float>>> &res);

#endif
