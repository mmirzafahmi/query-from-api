import json
from flask import Flask, request, jsonify
from lib.etl import extract_data_from_storage, preprocess

app = Flask(__name__)


@app.route('/')
def home():
    return "Hello Delivery Hero!"


@app.route('/api', methods=['POST'])
def api():
    if request.method == 'POST':

        # Input test
        assert len(request.args['fullVisitorId']) > 0, "Unable to process, empty string"
        assert request.args['fullVisitorId'].isdigit(), "It should be digits"

        full_visitor_id = request.args['fullVisitorId']
        # List of cloud storage URI
        urls = [
            "gs://product-analytics-hiring-tests-public/GoogleAnalyticsSample/ga_sessions_export/",
            "gs://product-analytics-hiring-tests-public/BackendDataSample/transactionalData/"
        ]

        # Extract and transform dataset into dataframe
        df_ga, df_td = [extract_data_from_storage(url) for url in urls]

        # Get Insight
        visitor_id, is_address_changed, is_placed_order, is_order_delivered, application_type = preprocess(
            df_ga,
            df_td,
            visitor_id=full_visitor_id
        )

        return jsonify({
            'fullVisitorId': visitor_id,
            'adddress_changed': repr(is_address_changed).lower(),
            'is_order_palced': repr(is_placed_order).lower(),
            'is_order_delivered': repr(is_order_delivered).lower(),
            'application_type': application_type
        })


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=7777, debug=True)

