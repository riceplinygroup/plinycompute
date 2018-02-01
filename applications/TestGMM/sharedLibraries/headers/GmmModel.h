/*****************************************************************************
 *                                                                           *
 *  Copyright 2018 Rice University                                           *
 *                                                                           *
 *  Licensed under the Apache License, Version 2.0 (the "License");          *
 *  you may not use this file except in compliance with the License.         *
 *  You may obtain a copy of the License at                                  *
 *                                                                           *
 *      http://www.apache.org/licenses/LICENSE-2.0                           *
 *                                                                           *
 *  Unless required by applicable law or agreed to in writing, software      *
 *  distributed under the License is distributed on an "AS IS" BASIS,        *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. *
 *  See the License for the specific language governing permissions and      *
 *  limitations under the License.                                           *
 *                                                                           *
 *****************************************************************************/
#ifndef GMM_MODEL_H
#define GMM_MODEL_H

#include "DoubleVector.h"
#include "GmmAggregateNewComp.h"
#include "Handle.h"
#include "LambdaCreationFunctions.h"
#include "Object.h"
#include "PDBVector.h"

#include <gsl/gsl_blas.h>
#include <gsl/gsl_linalg.h>
#include <gsl/gsl_matrix.h>
#include <gsl/gsl_permutation.h>
#include <gsl/gsl_vector.h>
#include <math.h>
#include <vector>

// By Tania, October 2017

using namespace pdb;

/* This class contains our current GMM model, that is:
 * - num of dimensions;
 * - weights: set of (prior) weights for each component
 * - means: mean for each component
 * - covars: covariance for each component
 * - inv_covars: inverse covariance for each component
 * - dets: determinant of each covariance matrix
 */
class GmmModel : public Object {

private:
  int ndim = 0;
  int k = 0;
  Handle<DoubleVector> weights;
  Handle<Vector<Handle<DoubleVector>>> means;      // k*ndim
  Handle<Vector<Handle<DoubleVector>>> covars;     // k*ndim*ndim
  Handle<Vector<Handle<DoubleVector>>> inv_covars; // k*ndim*ndim
  Handle<DoubleVector> dets;

public:
  ENABLE_DEEP_COPY

  GmmModel() {}

  GmmModel(int k, int ndim) {
    std::cout << "K= " << k << std::endl;
    std::cout << "ndim= " << ndim << std::endl;

    this->ndim = ndim;
    this->k = k;

    this->weights = pdb::makeObject<DoubleVector>(k);
    this->dets = pdb::makeObject<DoubleVector>(k);

    this->means = pdb::makeObject<Vector<Handle<DoubleVector>>>();
    this->covars = pdb::makeObject<Vector<Handle<DoubleVector>>>();
    this->inv_covars = pdb::makeObject<Vector<Handle<DoubleVector>>>();

    for (int i = 0; i < k; i++) {
      this->weights->setDouble(i, 1.0 / k);
      this->means->push_back(makeObject<DoubleVector>(ndim));
      this->covars->push_back(makeObject<DoubleVector>(ndim * ndim));
      this->inv_covars->push_back(makeObject<DoubleVector>(ndim * ndim));
    }

    std::cout << "Model created!" << std::endl;
  }

  // this prints the first 10 elements of a DoubleVector
  void print_vector(Handle<DoubleVector> v) {
    int MAX = 10;
    if (v->size < MAX)
      MAX = v->size;
    for (int i = 0; i < MAX; i++) {
      std::cout << i << ": " << v->getDouble(i) << "; ";
    }
    std::cout << std::endl;
  }

  // Print current model
  void print() {
    // Print mean
    std::cout << "**Current model**" << std::endl;
    std::cout << "Weights: ";
    print_vector(this->weights);
    for (int i = 0; i < k; i++) {
      std::cout << "**Component " << i << std::endl;
      std::cout << "Mean: ";
      print_vector((*this->means)[i]);
      std::cout << "Covar: ";
      print_vector((*this->covars)[i]);
      std::cout << "Inv covars: ";
      print_vector((*this->inv_covars)[i]);
    }
  }

  int getNumK() { return k; }

  int getNDim() { return ndim; }

  double getWeight(int i) { return this->weights->getDouble(i); }

  void updateWeights(std::vector<double> w) {
    for (int i = 0; i < k; i++) {
      this->weights->setDouble(i, w[i]);
    }
    std::cout << "updated weights";
    print_vector(this->weights);
  }

  void updateMeans(std::vector<std::vector<double>> &m) {
    for (int i = 0; i < k; i++) {
      std::cout << "updated mean for comp " << i << std::endl;

      double *mean = (*this->means)[i]->getRawData();
      for (int j = 0; j < ndim; j++) {
        mean[j] = m[i][j];
      }
      print_vector((*this->means)[i]);
    }
  }

  void updateCovars(std::vector<std::vector<double>> &c) {
    for (int i = 0; i < k; i++) {
      std::cout << "updated covar for comp " << i << std::endl;

      double *covar = (*this->covars)[i]->getRawData();

      for (int j = 0; j < (ndim * ndim); j++) {
        covar[j] = c[i][j];
      }
      print_vector((*this->covars)[i]);
    }

    // calcInvCovars();
  }

  // It calculates determinants and inverse covariances based on Cholesky
  // decomposition
  void calcInvCovars() {

    for (int i = 0; i < this->getNumK(); i++) {

      gsl_matrix_view covar =
          gsl_matrix_view_array((*covars)[i]->data->c_ptr(), ndim, ndim);
      gsl_matrix_view inv_covar =
          gsl_matrix_view_array((*inv_covars)[i]->data->c_ptr(), ndim, ndim);

      gsl_matrix_memcpy(&inv_covar.matrix, &covar.matrix);

      // gsl_permutation *p = gsl_permutation_alloc(ndim);

      gsl_linalg_cholesky_decomp(&inv_covar.matrix);

      // Calculate determinant -> logdet
      double ax = 0.0;
      for (int i = 0; i < ndim; i++) {
        ax += log(gsl_matrix_get(&inv_covar.matrix, i, i));
      }

      // Check for
      gsl_linalg_cholesky_invert(&inv_covar.matrix);

      dets->setDouble(i, ax);
    }
  }

  // It returns the rvalue, responsability, posterior probability of the
  // datapoint
  // for GMM component i
  double log_normpdf(int i, Handle<DoubleVector> inputData) {

    gsl_vector_view data =
        gsl_vector_view_array(inputData->data->c_ptr(), ndim);
    gsl_vector_view mean =
        gsl_vector_view_array((*means)[i]->data->c_ptr(), ndim);
    gsl_matrix_view covar =
        gsl_matrix_view_array((*covars)[i]->data->c_ptr(), ndim, ndim);
    gsl_matrix_view inv_covar =
        gsl_matrix_view_array((*inv_covars)[i]->data->c_ptr(), ndim, ndim);

    double ax, ay;
    int s;
    gsl_vector *ym;
    gsl_vector *datan = gsl_vector_alloc(ndim);

    ax = dets->getDouble(i);

    gsl_vector_memcpy(datan, &data.vector);
    gsl_vector_sub(datan, &mean.vector);

    ym = gsl_vector_alloc(ndim);
    gsl_blas_dsymv(CblasUpper, 1.0, &inv_covar.matrix, datan, 0.0, ym);
    gsl_blas_ddot(datan, ym, &ay);

    gsl_vector_free(ym);
    gsl_vector_free(datan);

    ay = -ax - 0.5 * ay;

    return ay;
  }

  // It calculates the sum of elements in vector v in log space
  // To do this, we extract the maximum factor (maxVal) from
  // all the values
  double logSumExp(double *v, size_t size) {

    double maxVal = v[0];
    double sum = 0;

    for (int i = 1; i < size; i++) {
      if (v[i] > maxVal) {
        maxVal = v[i];
      }
    }

    for (int i = 0; i < size; i++) {
      sum += exp(v[i] - maxVal);
    }

    return log(sum) + maxVal;
  }

  double logSumExp(DoubleVector &vector) {
    return logSumExp(vector.getRawData(), vector.size);
  }

  double logSumExp(Vector<double> &vector) {
    return logSumExp(vector.c_ptr(), vector.size());
  }

  // It uses the partial results from the Aggregate to update the model

  double updateModel(GmmAggregateNewComp update) {

    const double MIN_COVAR = 1e-6;

    double *sumWeights = update.getSumWeights().c_ptr();
    double totalSumR = 0.0;

    for (int i = 0; i < k; i++) {
      totalSumR += sumWeights[i];
    }

    // Update weights -> weights = sumR/totalSumR
    gsl_vector_view gSumWeights = gsl_vector_view_array(sumWeights, k);
    gsl_vector_view gweights =
        gsl_vector_view_array((*weights).data->c_ptr(), k);
    gsl_vector_memcpy(&gweights.vector, &gSumWeights.vector);
    gsl_vector_scale(&gweights.vector, 1.0 / totalSumR);

    // Update Mean and Covar for each component
    for (int i = 0; i < k; i++) {

      // Update means -> means = 1/sumR * weightedX
      gsl_vector_view mean =
          gsl_vector_view_array((*means)[i]->data->c_ptr(), ndim);
      gsl_vector_view gsumMean =
          gsl_vector_view_array(update.getSumMean(i).c_ptr(), ndim);
      gsl_vector_memcpy(&mean.vector, &gsumMean.vector);
      gsl_vector_scale(&mean.vector, 1.0 / sumWeights[i]);

      // Update covars -> gsumCovarv / sumR - mean*mean^T

      gsl_vector_view gsumCovarv =
          gsl_vector_view_array(update.getSumCovar(i).c_ptr(), ndim * ndim);
      gsl_vector_scale(&gsumCovarv.vector, 1.0 / sumWeights[i]);
      gsl_matrix_view gsumCovarm =
          gsl_matrix_view_array(update.getSumCovar(i).c_ptr(), ndim, ndim);
      gsl_blas_dsyr(CblasUpper, -1.0, &mean.vector, &gsumCovarm.matrix);

      // Add constant to diagonal and copy lower triangular
      double d;
      for (int row = 0; row < ndim; row++) {
        d = gsl_matrix_get(&gsumCovarm.matrix, row, row);
        gsl_matrix_set(&gsumCovarm.matrix, row, row, d + MIN_COVAR);

        for (int col = row + 1; col < ndim; col++) {
          // matrix[j][i] = matrix[i][j]
          d = gsl_matrix_get(&gsumCovarm.matrix, row, col);
          gsl_matrix_set(&gsumCovarm.matrix, col, row, d);
        }
      }

      gsl_vector_view covar =
          gsl_vector_view_array((*covars)[i]->data->c_ptr(), ndim * ndim);
      gsl_vector_memcpy(&covar.vector, &gsumCovarv.vector);
    }

    // Finally, calculate inverse covariances and determinants to be used in the
    // next iteration
    calcInvCovars();

    return update.getLogLikelihood();
  }

  ~GmmModel() {}
};

#endif
