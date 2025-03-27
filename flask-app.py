from flask import Flask, render_template, request, redirect, url_for, flash, get_object_or_404
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime

app = Flask(__name__)
app.secret_key = "your_secret_key"

app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///reservations.db"
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db = SQLAlchemy(app)

# Predefined users for the dropdown
PREDEFINED_USERS = ["Alice", "Bob", "Charlie", "Diana"]

# Reservation model
class Reservation(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    account_name = db.Column(db.String(80), nullable=False)
    requester_name = db.Column(db.String(120), nullable=False)
    start_time = db.Column(db.DateTime, nullable=False)
    end_time = db.Column(db.DateTime, nullable=False)

with app.app_context():
    db.create_all()

def is_overlapping(start, end, exclude_id=None):
    query = Reservation.query
    if exclude_id:
        query = query.filter(Reservation.id != exclude_id)
    overlapping = query.filter(
        db.or_(
            db.and_(Reservation.start_time <= start, Reservation.end_time > start),
            db.and_(Reservation.start_time < end, Reservation.end_time >= end),
            db.and_(Reservation.start_time >= start, Reservation.end_time <= end)
        )
    ).first()
    return overlapping is not None

@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        account_name = request.form.get("account_name")
        requester_name = request.form.get("requester_name")
        start_str = request.form.get("start_time")
        end_str = request.form.get("end_time")

        if not account_name or not requester_name or not start_str or not end_str:
            flash("All fields are required.", "error")
        else:
            try:
                start = datetime.strptime(start_str, "%Y-%m-%dT%H:%M")
                end = datetime.strptime(end_str, "%Y-%m-%dT%H:%M")
                if end <= start:
                    flash("End time must be after start time.", "error")
                elif is_overlapping(start, end):
                    flash("This time slot overlaps with an existing reservation.", "error")
                else:
                    new_res = Reservation(
                        account_name=account_name,
                        requester_name=requester_name,
                        start_time=start,
                        end_time=end
                    )
                    db.session.add(new_res)
                    db.session.commit()
                    flash("Reservation created successfully.", "success")
            except ValueError:
                flash("Invalid date/time format.", "error")

        return redirect(url_for("index"))

    reservations = Reservation.query.order_by(Reservation.start_time).all()
    return render_template("index.html", reservations=reservations, users=PREDEFINED_USERS)

@app.route("/edit/<int:res_id>", methods=["GET", "POST"])
def edit(res_id):
    reservation = get_object_or_404(Reservation, id=res_id)

    if request.method == "POST":
        account_name = request.form.get("account_name")
        requester_name = request.form.get("requester_name")
        start_str = request.form.get("start_time")
        end_str = request.form.get("end_time")

        try:
            start = datetime.strptime(start_str, "%Y-%m-%dT%H:%M")
            end = datetime.strptime(end_str, "%Y-%m-%dT%H:%M")

            if end <= start:
                flash("End time must be after start time.", "error")
            elif is_overlapping(start, end, exclude_id=reservation.id):
                flash("That time range overlaps with another reservation.", "error")
            else:
                reservation.account_name = account_name
                reservation.requester_name = requester_name
                reservation.start_time = start
                reservation.end_time = end
                db.session.commit()
                flash("Reservation updated successfully.", "success")
                return redirect(url_for("index"))
        except ValueError:
            flash("Invalid input format.", "error")

    return render_template("edit.html", reservation=reservation, users=PREDEFINED_USERS)

@app.route("/delete/<int:res_id>", methods=["POST"])
def delete(res_id):
    reservation = get_object_or_404(Reservation, id=res_id)
    db.session.delete(reservation)
    db.session.commit()
    flash("Reservation canceled.", "success")
    return redirect(url_for("index"))

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5001)
