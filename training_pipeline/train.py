import argparse
import os 
import hopsworks
import pandas as pd
from utils import data, utils
from utils.settings import ENV_VARS
from hsml.schema import Schema
from hsml.model_schema import ModelSchema
from feature_pipeline.ETL import load
from xgboost import XGBRegressor, plot_importance
from sklearn.model_selection import GridSearchCV
from sklearn.metrics import mean_squared_error, r2_score
from typing import Tuple
import matplotlib.pyplot as plt


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', '-v', type=int, default=1, help='Version for the feature groups.')
    parser.add_argument('--hyperparameter_tuning', '-ht', default=False, action='store_true', help='Decides if hyperparametertuning is performed or not.')
    parser.add_argument("--model_name", type=str, default='model_all_production_2', help='Name given when saving the model both locally and in Hopsworks.')
    # Mutually exclusive group:
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--total_production', '-tp', action='store_true', help='Defines how many and which features are used during training. In this case only the total production of energy is used.')
    group.add_argument('--all_production', '-ap', action='store_true', help='Defines how many and which features are used during training. In this case all types of energy produced per country are considered.')
    return parser


def get_training_data(version: int, total_production: bool, create_feature_view: bool = False) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    '''
    Loads the feature view from the feature store and splits the data into training and testing sets.
    '''
    feature_store = load.get_feature_store()

    if create_feature_view:
        feature_view = utils.create_feature_view(version)
    else:
        feature_view = feature_store.get_feature_view(
            name = 'cross_border_electricity_fv',
            version = version
        )
    X_train, X_test, y_train, y_test = data.split_training_data(feature_view)
    X_train_one_hot, X_test_one_hot = data.prepare_data_for_training(X_train, X_test, total_production) 
    return X_train_one_hot, X_test_one_hot, y_train, y_test


def train(hyperparameter_tuning: bool, X_train_one_hot: pd.DataFrame, y_train: pd.DataFrame) -> XGBRegressor:
    '''
    Trains the model using the training data.
    '''
    # Define the model
    xgb_regressor = XGBRegressor()
    if hyperparameter_tuning:
        # Define the hyperparameter grid
        param_grid = {
            'n_estimators': [100, 200, 300],
            'max_depth': [6, 7, 10],
            'learning_rate': [0.01, 0.1, 0.3],
            'subsample': [0.8, 1.0],
            'colsample_bytree': [0.8, 1.0]
        }

        grid_search = GridSearchCV(
            estimator=xgb_regressor,
            param_grid=param_grid,
            scoring='neg_mean_squared_error',
            cv=3,  # 3-fold cross-validation
            verbose=2,
            n_jobs=-1  # Use all available cores
        )

        # Perform hyperparameter tuning
        grid_search.fit(X_train_one_hot, y_train)

        # Retrieve the best model
        best_model = grid_search.best_estimator_
        return best_model 
    else: 
        xgb_regressor = XGBRegressor()
        xgb_regressor.fit(X_train_one_hot, y_train)
        return xgb_regressor


def evaluate(model, X_test: pd.DataFrame, y_test: pd.DataFrame) -> dict:
    '''
    Evaluates the model using the test data.
    ''' 
    y_pred = model.predict(X_test)
    # Calculating Mean Squared Error (MSE) using sklearn
    mse = mean_squared_error(y_test.iloc[:,0], y_pred)
    print("MSE:", mse)

    # Calculating R squared using sklearn
    r2 = r2_score(y_test.iloc[:,0], y_pred)
    print("R squared:", r2)
    return {"MSE": str(mse), "R squared": str(r2)}


def save_model(
    model, 
    locally: bool,
    model_name: str, 
    to_hopsworks: bool, 
    results_dict: dict, 
    X_train: pd.DataFrame,
    y_train: pd.DataFrame, 
    X_test: pd.DataFrame
) -> None:
    '''
    Saves the model locally and in Hopsworks.
    '''
    model_dir = "models"
    if not os.path.exists(model_dir):
        os.mkdir(model_dir)
    if locally:
        model.save_model(model_dir + f"/{model_name}.json")
    if to_hopsworks:
        project = hopsworks.login(api_key_value=ENV_VARS["HOPSWORKS"], project='cross_border_electricity')
        mr = project.get_model_registry()

        input_schema = Schema(X_train)
        output_schema = Schema(y_train)
        model_schema = ModelSchema(input_schema=input_schema, output_schema=output_schema)

        # Creating a Python model in the model registry 
        cross_border_model = mr.python.create_model(
            name=model_name, 
            metrics= results_dict,
            model_schema=model_schema,
            input_example=X_test.sample().values, 
            description="Trained XGBRegressor model for predicting cross-border electricity flows.",
        )
        # Saving the model artifacts in the model registry
        cross_border_model.save(model_dir)



def save_feature_importance_and_residual_plot(model, X_test: pd.DataFrame, model_name: str, num_features: int = 10) -> None:
    '''
    Saves the feature importance plot locally.
    '''
    images_dir = "models/images"
    if not os.path.exists(images_dir):
        os.makedirs(images_dir)

    # Feature importance plot
    plot_importance(model, max_num_features=num_features)
    feature_importance_path = os.path.join(images_dir, f'feature_importance_{model_name}.png')
    plt.savefig(feature_importance_path)
    plt.clf()  # Clear the figure for the next plot

    # Residual plot
    plt.figure(figsize=(10, 6))
    y_pred = model.predict(X_test)
    residuals = y_test.iloc[:, 0] - y_pred
    plt.scatter(y_test.iloc[:, 0], residuals)
    plt.axhline(y=0, color='black', linestyle='--')
    plt.xlabel("Actual values")
    plt.ylabel("Residuals")
    plt.title("Residual plot")
    residual_plot_path = os.path.join(images_dir, f'residual_plot_{model_name}.png')
    plt.savefig(residual_plot_path)
    plt.clf()


if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()
    version = args.version
    total_production = args.total_production
    hyperparameter_tuning = args.hyperparameter_tuning
    model_name = args.model_name

    # Train the model
    print("Starting training...")
    if args.total_production:
        X_train_one_hot, X_test_one_hot, y_train, y_test = get_training_data(version, True)
    else:
        X_train_one_hot, X_test_one_hot, y_train, y_test = get_training_data(version, False)
    
    model = train(hyperparameter_tuning, X_train_one_hot, y_train)

    # Evaluate the model
    print("Evaluating the model...")
    results_dict = evaluate(model, X_test_one_hot, y_test)

    # Save the model     
    save_model(
        model=model,
        locally=True,
        model_name=model_name,
        to_hopsworks=True,
        results_dict=results_dict,
        X_train=X_train_one_hot,
        y_train=y_train,
        X_test=X_test_one_hot
    )
    # Save the feature importance plot and residual plot
    save_feature_importance_and_residual_plot(model, X_test_one_hot, model_name)

    print(f"Model saved locally and to Hopsworks with name '{model_name}'.") 
