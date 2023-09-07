from flask import jsonify

class CustomException(Exception):

    def __init__(self, status_code, name = "Custom Error", description = 'Error'): 
        super().__init__()
        self.description = description
        self.name = name
        self.status_code = status_code

    def get_response(self):
        response = jsonify({
            'error': {
                'code': self.status_code,
                'name': self.name,
                'description': self.description,
            }
        })
        response.status_code = self.status_code
        return response
    


class FilmNotFound(CustomException):
    
    def __init__(self, name, description):
        super().__init__(404, name, description)

class InvalidDataError(CustomException):
    
    def __init__(self,name, description):
        super().__init__(400,name, description)