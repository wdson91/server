import datetime
from functools import wraps
import jwt
from flask import request, jsonify

def decode_supabase_token_sem_verificacao(token):
    try:
        return jwt.decode(token, options={"verify_signature": False})
    except Exception as e:
        raise ValueError(f"Erro ao decodificar token: {e}")

def require_valid_token(view_func):
    @wraps(view_func)
    def wrapped_view(*args, **kwargs):
        auth_header = request.headers.get("Authorization")
        

        if not auth_header or not auth_header.startswith("Bearer "):
            return jsonify({"error": "Token ausente ou malformado"}), 401

        token = auth_header.split(" ")[1]

        try:
            payload = decode_supabase_token_sem_verificacao(token)
            exp_timestamp = payload.get("exp")
            now_timestamp = int(datetime.datetime.utcnow().timestamp())
            

            if exp_timestamp is None:
                return jsonify({"error": "Token sem campo 'exp'"}), 400

            if exp_timestamp < now_timestamp:
                exp_date = datetime.datetime.utcfromtimestamp(exp_timestamp).strftime("%Y-%m-%d %H:%M:%S UTC")
                return jsonify({
                    "error": "Token expirado",
                    "expirou_em": exp_date
                }), 401

            # Injeta o user_id no objeto global `g`
            from flask import g
            g.user_id = payload.get("sub")

            return view_func(*args, **kwargs)

        except ValueError as e:
            return jsonify({"error": str(e)}), 400

    return wrapped_view
