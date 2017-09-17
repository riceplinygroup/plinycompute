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

#include "Object.h"
#include "PDBVector.h"
#include "Handle.h"
#include "LambdaCreationFunctions.h"
#include "DoubleVector.h"
#include "GMM/GmmNewComp.h"

#include <vector>
#include <math.h>
#include <gsl/gsl_vector.h>
#include <gsl/gsl_matrix.h>
#include <gsl/gsl_permutation.h>
#include <gsl/gsl_linalg.h>
#include <gsl/gsl_blas.h>

// PRELOAD %GmmModel%

// By Tania, August 2017

using namespace pdb;

class GmmModel : public Object {
	/* This class contains our current GMM model, that is:
	 * - num of dimensions;
	 * - weights: set of (prior) weights for each component
	 * - means: mean for each component
	 * - covars: covariance for each component
	 * - inv_covars: inverse covariance for each component
	 */

private:
	int ndim = 0;
    int k = 0;
    Handle<DoubleVector> weights;
    Handle<Vector<Handle<DoubleVector>>> means; //k*ndim
    Handle<Vector<Handle<DoubleVector>>> covars; //k*ndim*ndim
    Handle<Vector<Handle<DoubleVector>>> inv_covars; //k*ndim*ndim
    Handle<DoubleVector> dets;


public:

	ENABLE_DEEP_COPY

	GmmModel () {}

	GmmModel (int k, int ndim) {
		std::cout << "K= " << k << std::endl;
		std::cout << "ndim= " << ndim << std::endl;

		this->ndim = ndim;
		this->k = k;

		//pdb::Handle<pdb::Vector<Handle<DoubleVector>>> my_model = pdb::makeObject<pdb::Vector<Handle<DoubleVector>>> ();

		this->weights = pdb::makeObject<DoubleVector>(k);
		this->dets = pdb::makeObject<DoubleVector>(k);

		this->means = pdb::makeObject<Vector<Handle<DoubleVector>>>();
		this->covars = pdb::makeObject<Vector<Handle<DoubleVector>>>();
		this->inv_covars = pdb::makeObject<Vector<Handle<DoubleVector>>>();

		for (int i=0; i<k; i++) {
			this->weights->setDouble(i,1.0/k);
			this->means->push_back(makeObject<DoubleVector>(ndim));
			this->covars->push_back(makeObject<DoubleVector>(ndim*ndim));
			this->inv_covars->push_back(makeObject<DoubleVector>(ndim*ndim));
		}

		std::cout << "Model created!" << std::endl;
	}


	void print_vector(Handle<DoubleVector> v) {
		 int MAX = 10;
		 if (v->size < MAX) MAX = v->size;
		 for (int i = 0; i < MAX; i++) {
			 std :: cout << i << ": " << v->getDouble(i) << "; ";
		 }
		 std :: cout << std :: endl;
	 }

	void print() {
		//Print mean
		std::cout << "**Current model**" << std::endl;
		std::cout << "Weights: ";
		print_vector(this->weights);
		for (int i = 0; i < k; i++) {
			std::cout <<"**Component " << i << std::endl;
			std::cout <<"Mean: ";
			print_vector((*this->means)[i]);
			std::cout <<"Covar: ";
			//(*this->covars)[i]->print();
			print_vector((*this->covars)[i]);
			std::cout <<"Inv covars: ";
			print_vector((*this->inv_covars)[i]);


		}
	}

	int getNumK(){
			return k;
		}

	int getNDim(){
		return ndim;
	}

	double getWeight(int i){
		return this->weights->getDouble(i);
	}

	void updateWeights (std::vector<double> w){
		for (int i=0; i<k; i++){
			this->weights->setDouble(i,w[i]);
		}
		std::cout << "updated weights";
		print_vector(this->weights);

	}


	void updateMeans(std::vector<std::vector<double>>& m){
		for (int i=0; i<k; i++){
			std::cout << "updated mean for comp " << i << std::endl;

			double* mean = (*this->means)[i]->getRawData();
			for (int j=0; j<ndim; j++){
				mean[j] = m[i][j];
			}
			print_vector((*this->means)[i]);
		}
	}

	void updateCovars(std::vector<std::vector<double>>& c){
		for (int i=0; i<k; i++){
			std::cout << "updated covar for comp " << i << std::endl;

			double* covar = (*this->covars)[i]->getRawData();

			for (int j=0; j<(ndim*ndim); j++){
				covar[j] = c[i][j];
			}
			print_vector((*this->covars)[i]);
		}

		//calcInvCovars();
	}

	double my_gsl_linalg_LU_det (gsl_matrix * LU, int signum)
	{
	  size_t i, n = LU->size1;

	  double det = (double) signum;

	  for (i = 0; i < n; i++)
	    {
	      det *= gsl_matrix_get (LU, i, i);

	    }

	  return det;
	}

	void calcInvCovars(){

		std::cout <<std::endl;
		std::cout << "************+Calculating covars" <<std::endl;

		this->print(); //Print model

		std::cout <<std::endl;

		for (int i = 0; i<this->getNumK(); i++){
			//gsl_matrix* cov = gsl_matrix_calloc(ndim,ndim);

			gsl_matrix_view covar = gsl_matrix_view_array((*covars)[i]->data->c_ptr(), ndim, ndim);
			gsl_matrix_view inv_covar = gsl_matrix_view_array((*inv_covars)[i]->data->c_ptr(), ndim, ndim);

			gsl_matrix_memcpy (&inv_covar.matrix, &covar.matrix);

			//gsl_permutation *p = gsl_permutation_alloc(ndim);


			gsl_linalg_cholesky_decomp(&inv_covar.matrix);

			//Calculate determinant -> logdet
			double ax = 0.0;
			for (int i=0;i<ndim;i++) {
				ax += log(gsl_matrix_get(&inv_covar.matrix, i, i));
			}


			//Check for
			gsl_linalg_cholesky_invert(&inv_covar.matrix);


			dets->setDouble(i,ax);
			std::cout << "ax " << i << " " << ax << std::endl;


			//gsl_permutation_free(p);
			//gsl_matrix_free(cov);


			//gsl_linalg_LU_decomp(cov, p, &s);
			//gsl_linalg_LU_invert(cov, p, &inv_covar.matrix);


			/*
			 * https://github.com/apache/spark/blob/master/mllib-local/src/main/scala/org/apache/spark/ml/stat/distribution/MultivariateGaussian.scala
			 * To guard against singular covariance matrices, this method computes both the
			   * pseudo-determinant and the pseudo-inverse (Moore-Penrose).  Singular values are considered
			   * to be non-zero only if they exceed a tolerance based on machine precision, matrix size, and
			   * relation to the maximum singular value (same tolerance used by, e.g., Octave).
			   *
			   *
			   * For numerical stability, values are considered to be non-zero only if they exceed tol.
				This prevents any inverted value from exceeding (eps * n * max(d))^-1
				   val tol = Utils.EPSILON * max(d) * d.length


	    	const double MIN_COVAR = 2.22044604925e-16;

	    	double d;
	    	double max = gsl_matrix_get(cov,0,0);
	    	for (int row=1; row<ndim; row++){
	    		d = gsl_matrix_get(cov,row,row);
				if (d > max) {
					max = d;
				}
	    	}

			//double tol = MIN_COVAR * gsl_matrix_max(cov) * (ndim*ndim);
	    	double tol = MIN_COVAR * max * (ndim*ndim);

			std::cout << "tol=" << tol << std::endl;

			double det = 0.0;//(double) s;

			for (int row=0; row<ndim; row++){
				d = gsl_matrix_get(cov,row,row);
				if (d > tol) {
					det += log(d);
					std::cout << "det " << i << " " << det   << " d " << d << " log(d)" << log(d) << std::endl;
				}
			}

			ax = exp(det); */

			//ax = gsl_linalg_LU_det(&covar.matrix, s);
			//ax = my_gsl_linalg_LU_det(cov, s);
			//ax = gsl_linalg_LU_det(cov, s);




			//std::cout << "covars " << i << std::endl; (*covars)[i]->print();
			//std::cout << "inv_covars" << i << std::endl; (*inv_covars)[i]->print();;

		}

		std::cout << "dets calculated" << std::endl; dets->print();

	}

	double log_normpdf(int i, Handle<DoubleVector> inputData, bool isLog) {

		//std::cout << "means " << i << std::endl; (*means)[i]->print();
		//std::cout << "covars " << i << std::endl; (*covars)[i]->print();
		//std::cout << "inv_covars" << i << std::endl; (*inv_covars)[i]->print();
		//std::cout << "dets" << i << std::endl; dets->print();

		//std::cout << "means " << i << std::endl; print_vector(inputData);



		gsl_vector_view data = gsl_vector_view_array(inputData->data->c_ptr(), ndim);
		gsl_vector_view mean = gsl_vector_view_array((*means)[i]->data->c_ptr(), ndim);
		gsl_matrix_view covar = gsl_matrix_view_array((*covars)[i]->data->c_ptr(), ndim, ndim);
		gsl_matrix_view inv_covar = gsl_matrix_view_array((*inv_covars)[i]->data->c_ptr(), ndim, ndim);


		double ax, ay;
		int s;
		gsl_vector *ym;
		gsl_vector *datan = gsl_vector_alloc(ndim);

		ax = dets->getDouble(i);

		//ax = 1.00498e-15;
		gsl_vector_memcpy(datan, &data.vector);
		gsl_vector_sub(datan, &mean.vector);

		ym = gsl_vector_alloc(ndim);

		//std::cout << "inv_covars BEFORE" << i << std::endl; (*inv_covars)[i]->print();

		gsl_blas_dsymv(CblasUpper, 1.0, &inv_covar.matrix, datan, 0.0, ym);


	  /*gsl_blas_dsymv(CblasUpper, 1.0, gsl_covariance, gsl_data, 0.0, ym);

	  double ay;
	  gsl_blas_ddot(gsl_data, ym, &ay);*/

		//std::cout << "covars AFTER" << i << std::endl; (*inv_covars)[i]->print();

		gsl_blas_ddot(datan, ym, &ay);


		gsl_vector_free(ym);
		gsl_vector_free(datan);

		//std::cout << "ax " << ax << std::endl;

		//std::cout << "exp(-0.5*ay) " << exp(-0.5*ay) << std::endl;

		ay = -ax - 0.5*ay;

				//exp(-0.5*ay)/sqrt(pow(((double)2*3.1415926), (double)ndim)*ax);

		//std::cout << "ay " << ay << std::endl;


		//std::cout << "data " << i << std::endl; inputData->print();
		//std::cout << "means " << i << std::endl; (*means)[i]->print();
		//std::cout << "ay " << ay << std::endl;

		if (!isLog){
			return exp(ay);
		}
		return ay;//log10(ay);
	}



	double logSumExp(DoubleVector vector)
	{
		double* v = vector.getRawData();
		size_t size = vector.size;
		double maxVal = v[0];
		double sum = 0;

		for (int i = 1 ; i < size ; i++){
			if (v[i] > maxVal){
				maxVal = v[i];
			}
		}

		for (int i = 0; i < size ; i++){
			sum += exp(v[i] - maxVal);
		}

		return log(sum) + maxVal;

	   //e = log(sum(exp(v-maxVal))) + maxVal
	}


	void updateModel(GmmNewComp update){

    	//const double MIN_COVAR = 1e-5;
    	//const double MIN_COVAR = 2.22044604925e-16;
		const double MIN_COVAR = 1e-6;

		double* sumR = update.getSumR().getRawData();
		double totalSumR = 0.0;
		for (int i=0; i<k; i++){
			totalSumR += sumR[i];
		}


		//std::cout << "gsumR"; update.getSumR().print();

		//Update weights -> weights = sumR/totalSumR
		gsl_vector_view gsumR = gsl_vector_view_array(update.getSumR().data->c_ptr(), k);
		gsl_vector_view gweights = gsl_vector_view_array((*weights).data->c_ptr(), k);
		gsl_vector_memcpy (&gweights.vector, &gsumR.vector);
		gsl_vector_scale (&gweights.vector, 1.0/totalSumR);



		/*
		 * The thing is that NaNs naturally creep in when you have certain clusters with few points.  This happens readily in high dimensions.

		The "real" solution is that when you find a cluster whose covariance matrix is not positive definite is that you take the cluster with the highest total probability and split it.  That is, if the centroid is Mu with Cov as the covariance matrix, do:

		for i = 1 to numDims
     	 	 newMu = Mu + Normal (0, Cov[i][i] / 1000)

		Then newCov = Cov, and newProb = prob / 2 (where prob is the probability of the cluster you are splitting).  This is done when you update the model.
		 *
		 */

		//Update Mean and Covar for each component
		for (int i=0; i<k; i++){

			//std::cout << "mean A" << i << std::endl; update.getWeightedX(i).print();

			//Update means -> means = 1/sumR * weightedX
			gsl_vector_view mean = gsl_vector_view_array((*means)[i]->data->c_ptr(), ndim);
			gsl_vector_view gweightedX = gsl_vector_view_array(update.getWeightedX(i).data->c_ptr(), ndim);
			gsl_vector_memcpy (&mean.vector, &gweightedX.vector);
			gsl_vector_scale (&mean.vector, 1.0/sumR[i]);


			//std::cout << "mean B" << i << std::endl; (*means)[i]->print();


			//Update covars -> weightedX2 / sumR - mean*mean^T + MIN_COVAR
			//According to PYTHON GMM https://github.com/FlytxtRnD/GMM/blob/master/GMMclustering.py

			//std::cout << "gweightedX2 A" << i << std::endl; update.getWeightedX2(i).print();

			gsl_vector_view gweightedX2v = gsl_vector_view_array(update.getWeightedX2(i).data->c_ptr(), ndim*ndim);
			gsl_vector_scale (&gweightedX2v.vector, 1.0/sumR[i]);

			//std::cout << "gweightedX2 B" << i << std::endl; update.getWeightedX2(i).print();

			gsl_matrix_view gweightedX2m = gsl_matrix_view_array(update.getWeightedX2(i).data->c_ptr(), ndim, ndim);
			gsl_blas_dsyr (CblasUpper, -1.0, &mean.vector, &gweightedX2m.matrix);

			//std::cout << "gweightedX2 C" << i << std::endl; update.getWeightedX2(i).print();


			//Add constant to diagonal
			//Copy lower triangular
			double d;
			for (int row=0; row<ndim; row++){
				d = gsl_matrix_get(&gweightedX2m.matrix,row,row);
				gsl_matrix_set(&gweightedX2m.matrix,row,row, d + MIN_COVAR);

				for (int col=row+1; col<ndim; col++){
					//matrix[j][i] = matrix[i][j]
					d = gsl_matrix_get(&gweightedX2m.matrix,row,col);
					gsl_matrix_set(&gweightedX2m.matrix,col,row, d);
				}
			}

			//std::cout << "gweightedx2 D" << i << std::endl; update.getWeightedX2(i).print();

			gsl_vector_view covar = gsl_vector_view_array((*covars)[i]->data->c_ptr(), ndim*ndim);
			gsl_vector_memcpy (&covar.vector, &gweightedX2v.vector);

			//std::cout << "gweightedx2 E" << i << std::endl; (*covars)[i]->print();

		}

		calcInvCovars();

	}



    ~GmmModel () {
    	/*for (int i; i<getNumK(); i++) {
    		(*means)[i]->deleteObject();
    		gsl_matrix_free(covars[i]);
    		gsl_matrix_free(inv_covars[i]);
		}

    	(*means).clear();
    	(*covars).clear();
    	(*inv_covars).clear();
    	(*weights.)clear();*/
    }

};


#endif





