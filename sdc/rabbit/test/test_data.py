test_secret = "seB388LNHgxcuvAcg1pOV20_VR7uJWNGAznE0fOqKxg=".encode('ascii')

test_data = {
    'valid':
        '{"tx_id":"0f534ffc-9442-414c-b39f-a756b4adc6cb","collection":'
        '{"exercise_sid":"hfjdskf"},"metadata":{"user_id":"789473423","ru_ref":"12345678901A"}}',  # noqa
    'invalid': '{"cats":"are nice"}',
    'missing_metadata': '{"tx_id":"0f534ffc-9442-414c-b39f-a756b4adc6cb","collection":{"exercise_sid":"hfjdskf"}}',  # noqa
    'missing_ru_ref':
        '{"tx_id":"0f534ffc-9442-414c-b39f-a756b4adc6cb","collection":'
        '{"exercise_sid":"hfjdskf"},"metadata":{"user_id":"789473423"}}'
}
