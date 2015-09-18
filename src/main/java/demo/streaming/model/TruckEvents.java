package demo.streaming.model;

import java.io.Serializable;
import java.util.Date;

public class TruckEvents implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -3996207894989167715L;

	private String truckId;
	
	private Date eventTime;
	
	private String truckDriverId;
	
	private String eventDetail;
	
	private double latitudeId;
	
	private double longitudeId;

	public String getTruckId() {
		return truckId;
	}

	public void setTruckId(String truckId) {
		this.truckId = truckId;
	}

	public Date getEventTime() {
		return eventTime;
	}

	public void setEventTime(Date eventTime) {
		this.eventTime = eventTime;
	}

	public String getTruckDriverId() {
		return truckDriverId;
	}

	public void setTruckDriverId(String truckDriverName) {
		this.truckDriverId = truckDriverName;
	}

	public String getEventDetail() {
		return eventDetail;
	}

	public void setEventDetail(String eventDetail) {
		this.eventDetail = eventDetail;
	}

	public double getLatitudeId() {
		return latitudeId;
	}

	public void setLatitudeId(double latitudeId) {
		this.latitudeId = latitudeId;
	}

	public double getLongitudeId() {
		return longitudeId;
	}

	public void setLongitudeId(double longitudeId) {
		this.longitudeId = longitudeId;
	}
	
	@Override
	public String toString(){
		return(this.eventTime.toString()+":"
				+this.truckId + ":"
				+this.truckDriverId + ":"
				+this.eventDetail+":"
				+this.latitudeId+":"
				+this.longitudeId);
	}
}
