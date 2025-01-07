import argparse
from feature_pipeline.ETL import load
from utils import data
from xgboost import XGBRegressor
from sklearn.model_selection import GridSearchCV
from sklearn.metrics import mean_squared_error, r2_score
import os 
import hopsworks
from utils.settings import ENV_VARS
from hsml.schema import Schema
from hsml.model_schema import ModelSchema


def get_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', '-v', type=int, default=1, help='Version for the feature groups.')
    parser.add_argument('--hyperparameter_tuning', '-ht', default=False, action='store_true', help='Decides if hyperparametertuning is performed or not.')
    parser.add_argument("--model_name", type=str, default='model_total_production', help='Name given when saving the model both locally and in Hopsworks.')
    # Mutually exclusive group: backfill (-b) or daily (-d) is required
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--total_production', '-tp', action='store_true', help='Defines how many and which features are used during training. In this case only the total production of energy is used.')
    group.add_argument('--all_production', '-ap', action='store_true', help='Defines how many and which features are used during training. In this case all types of energy produced per country are considered.')
    return parser


def get_training_data(version:int, total_production:bool):
    feature_store = load.get_feature_store()

    feature_view = feature_store.get_feature_view(
        name = 'cross_border_electricity_fv',
        version = version
    )
    X_train, X_test, y_train, y_test = data.split_training_data(feature_view)
    X_train_one_hot, X_test_one_hot = data.prepare_data(X_train, X_test, total_production) 
    return X_train_one_hot, X_test_one_hot, y_train, y_test


def train(hyperparameter_tuning:bool, X_train_one_hot, y_train):
    # Define the model
    xgb_regressor = XGBRegressor()
    if hyperparameter_tuning:
        # Define the hyperparameter grid
        param_grid = {
            'n_estimators': [100, 200, 300],
            'max_depth': [3, 5, 6, 7, 10, 20],
            'learning_rate': [0.01, 0.05, 0.1, 0.2, 0.3],
            'subsample': [0.8, 0.9, 1.0],
            'colsample_bytree': [0.8, 0.9, 1.0]
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

def evaluate(model, x_test, y_test): 
    y_pred = model.predict(x_test)
    # Calculating Mean Squared Error (MSE) using sklearn
    mse = mean_squared_error(y_test.iloc[:,0], y_pred)
    print("MSE:", mse)

    # Calculating R squared using sklearn
    r2 = r2_score(y_test.iloc[:,0], y_pred)
    print("R squared:", r2)
    return {"MSE": str(mse), "R squared": str(r2)}

def save_model(model, locally:bool, model_name:str, to_hopsworks:bool, results_dict:dict, X_train, y_train, X_test):
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
    print(f"Model saved locally and to Hopsworks with name '{model_name}'.") 
