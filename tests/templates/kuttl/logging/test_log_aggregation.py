#!/usr/bin/env python3
import requests


def check_processed_events():
    response = requests.post(
        'http://hdfs-vector-aggregator:8686/graphql',
        json={
            'query': """
                {
                    transforms(first:100) {
                        nodes {
                            componentId
                            metrics {
                                processedEventsTotal {
                                    processedEventsTotal
                                }
                            }
                        }
                    }
                }
            """
        }
    )

    assert response.status_code == 200, \
        'Cannot access the API of the vector aggregator.'

    result = response.json()

    transforms = result['data']['transforms']['nodes']
    for transform in transforms:
        componentId = transform['componentId']
        processedEvents = transform['metrics']['processedEventsTotal']
        if componentId == 'filteredInvalidEvents':
            assert processedEvents is None or \
                processedEvents['processedEventsTotal'] == 0, \
                'Invalid log events were processed.'
        else:
            assert processedEvents is not None and \
                processedEvents['processedEventsTotal'] > 0, \
                f'No events were processed in "{componentId}".'


if __name__ == '__main__':
    check_processed_events()
    print('Test successful!')
