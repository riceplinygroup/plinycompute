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
		 if (ndim < MAX) MAX = ndim;
		 for (int i = 0; i < MAX; i++) {
			 std :: cout << i << ": " << v->getDouble(i) << "; ";
		 }
		 std :: cout << std :: endl;
	 }

	void print() {
		//Print mean
		std::cout << "**Current model**" << std::endl;
		std::cout << "Weights: ";
		//print_vector(this->weights);
		for (int i = 0; i < k; i++) {
			std::cout <<"**Component " << i << std::endl;
			std::cout <<"Mean: ";
			print_vector((*this->means)[i]);
			std::cout <<"Covar: ";
			print_vector((*this->covars)[i]);


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


	void updateMeans(std::vector<std::vector<double>> m){
		for (int i=0; i<k; i++){
			std::cout << "updated mean for comp " << i << std::endl;
			for (int j=0; j<ndim; j++){
				(*this->means)[i]->setDouble(j,m[i][j]);
			}
			print_vector((*this->means)[i]);
		}
	}

	void updateCovars(std::vector<std::vector<double>> c){
		for (int i=0; i<k; i++){
			std::cout << "updated covar for comp " << i << std::endl;
			for (int j=0; j<(ndim*ndim); j++){
				(*this->covars)[i]->setDouble(j,c[i][j]);
			}
			print_vector((*this->covars)[i]);
		}

		calcInvCovars();
	}


	void calcInvCovars(){
		std::cout << "Calculating covars" <<std::endl;

		int s;
		double ax;


		for (int i = 0; i<this->getNumK(); i++){
			gsl_matrix* cov = gsl_matrix_calloc(ndim,ndim);

			gsl_matrix_view covar = gsl_matrix_view_array((*covars)[i]->data->c_ptr(), ndim, ndim);
			gsl_matrix_view inv_covar = gsl_matrix_view_array((*inv_covars)[i]->data->c_ptr(), ndim, ndim);
			gsl_matrix_memcpy (cov, &covar.matrix);

			gsl_permutation *p = gsl_permutation_alloc(ndim);

			gsl_linalg_LU_decomp(cov, p, &s);
			gsl_linalg_LU_invert(cov, p, &inv_covar.matrix);

			ax = gsl_linalg_LU_det(&covar.matrix, s);
			dets->setDouble(i,ax);
			//std::cout << "ax " << i << " " << ax << std::endl;


			gsl_permutation_free(p);
			gsl_matrix_free(cov);

			//std::cout << "covars " << i << std::endl; (*covars)[i]->print();
			//std::cout << "inv_covars" << i << std::endl; (*inv_covars)[i]->print();;

		}

		std::cout << "dets calculated" << std::endl; dets->print();

	}

	double log_normpdf(int i, Handle<DoubleVector> inputData, bool isLog) {

		/*std::cout << "means " << i << std::endl; (*means)[i]->print();
		std::cout << "covars " << i << std::endl; (*covars)[i]->print();
		std::cout << "inv_covars" << i << std::endl; (*inv_covars)[i]->print();
		std::cout << "dets" << i << std::endl; dets->print();*/




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

		//std::cout << "covars AFTER" << i << std::endl; (*inv_covars)[i]->print();

		gsl_blas_ddot(datan, ym, &ay);


		gsl_vector_free(ym);
		gsl_vector_free(datan);

		//std::cout << "ax " << ax << std::endl;

		//std::cout << "exp(-0.5*ay) " << exp(-0.5*ay) << std::endl;

		ay = exp(-0.5*ay)/sqrt(pow(((double)2*3.1415926), (double)ndim)*ax);

		//std::cout << "ay " << ay << std::endl;


		if (!isLog){
			return (ay);
		}
		return log(ay);//log10(ay);
	}

	double logSumExp(DoubleVector v)
	{
	   if(v.size > 0 ){
	      double maxVal = v.getDouble(0);
	      double sum = 0;

	      for (int i = 1 ; i < v.size ; i++){
	         if (v.getDouble(i) > maxVal){
	            maxVal = v.getDouble(i);
	         }
	      }

	      for (int i = 0; i < v.size ; i++){
	         sum += exp(v.getDouble(i) - maxVal);
	      }
	      double result = log(sum) + maxVal;
	      return result;
	   }
	   else
	   {
	      return 0.0;
	   }

	   //e = log(sum(exp(v-maxVal))) + maxVal
	}


	void updateModel(GmmNewComp update){

    	const double MIN_COVAR = 1e-3;

		double totalSumR = 0.0;
		for (int i=0; i<k; i++){
			totalSumR += update.getR(i);
		}

		//Update weights -> weights = sumR/totalSumR
		gsl_vector_view gsumR = gsl_vector_view_array(update.getSumR().data->c_ptr(), ndim);
		gsl_vector_view gweights = gsl_vector_view_array((*weights).data->c_ptr(), ndim);
		gsl_vector_memcpy (&gweights.vector, &gsumR.vector);
		gsl_vector_scale (&gweights.vector, 1.0/totalSumR);

		//Update Mean and Covar for each component
		for (int i=0; i<k; i++){

			//Update means -> means = 1/sumR * weightedX
			gsl_vector_view mean = gsl_vector_view_array((*means)[i]->data->c_ptr(), ndim);
			gsl_vector_view gweightedX = gsl_vector_view_array(update.getWeightedX(i).data->c_ptr(), ndim);
			gsl_vector_memcpy (&mean.vector, &gweightedX.vector);
			gsl_vector_scale (&mean.vector, 1.0/update.getR(i));



			//Update covars -> weightedX2 / sumR - mean*mean^T + MIN_COVAR
			//According to PYTHON GMM https://github.com/FlytxtRnD/GMM/blob/master/GMMclustering.py

			/*gsl_vector_view gweightedX2v = gsl_vector_view_array(update.getWeightedX2(i).data->c_ptr(), ndim*ndim);
			gsl_vector_scale (&gweightedX2v.vector, 1.0/update.getR(i));

			gsl_matrix_view gweightedX2m = gsl_matrix_view_array(update.getWeightedX2(i).data->c_ptr(), ndim, ndim);
			gsl_blas_dsyr (CblasUpper, -1.0, &mean.vector, &gweightedX2m.matrix);

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

			gsl_vector_view covar = gsl_vector_view_array((*covars)[i]->data->c_ptr(), ndim*ndim);
			gsl_vector_memcpy (&covar.vector, &gweightedX2v.vector);*/

			// According to SCALA GMM MLLIB

			/* val mu = (mean /= weight)
				BLAS.syr(-weight, Vectors.fromBreeze(mu),
				Matrices.fromBreeze(sigma).asInstanceOf[DenseMatrix])
				val newWeight = weight / sumWeights
				val newGaussian = new MultivariateGaussian(mu, sigma / weight)
			 */

			gsl_matrix_view gweightedX2 = gsl_matrix_view_array(update.getWeightedX2(i).data->c_ptr(), ndim, ndim);

			//gsl_blas_dsyr (CBLAS_UPLO_t Uplo, double alpha, const gsl_vector * x, gsl_matrix * A) - //A = \alpha x x^T + A
			//update A = \alpha x x^T + A
			//gweightedX2 - r * mean mean^T
			gsl_blas_dsyr (CblasUpper, -update.getR(i), &mean.vector, &gweightedX2.matrix);

			//Copy lower triangular
			for (int row=0; row<ndim; row++){
				for (int col=row+1; col<ndim; col++){
					//matrix[j][i] = matrix[i][j]
					double d = gsl_matrix_get(&gweightedX2.matrix,row,col);
					gsl_matrix_set(&gweightedX2.matrix,col,row, d);
				}
			}

			gsl_vector_view gweightedX2v = gsl_vector_view_array(update.getWeightedX2(i).data->c_ptr(), ndim*ndim);
			gsl_vector_view covar = gsl_vector_view_array((*covars)[i]->data->c_ptr(), ndim*ndim);
			gsl_vector_memcpy (&covar.vector, &gweightedX2v.vector);

			gsl_vector_scale (&covar.vector, 1.0/(*weights).getDouble(i));
			gsl_vector_add_constant(&covar.vector, MIN_COVAR);
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





