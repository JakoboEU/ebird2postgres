package ebird2postgres.repository;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLType;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.Optional;

import ebird2postgres.ebird.EBirdRecord;

public class Checklist extends ChecklistStub {
	private final HotspotStub hotspot;
	private final Integer numberOfObservers;
	private final String groupId;
	private final LocalDate observationDate;
	private final LocalTime startTime;
	private final boolean isComplete;
	private final Double effortDistanceKm;
	private final Double effortAreaHa;
	private final Integer duration;
	private final String protocolType;
	private final String protocolCode;
	
	Checklist(EBirdRecord ebirdRecord, HotspotStub hotspot) {
		this(
				ebirdRecord.getChecklistId(),
				hotspot,
				ebirdRecord.getNumberOfObservers(),
				ebirdRecord.getGroupId(),
				ebirdRecord.getObservationDate(),
				ebirdRecord.getStartTime(),
				ebirdRecord.isComplete(),
				ebirdRecord.getEffortDistanceKm(),
				ebirdRecord.getEffortAreaHa(),
				ebirdRecord.getDuration(),
				ebirdRecord.getProtocolType(),
				ebirdRecord.getProtocolCode()
		);
	}
	
	private Checklist(String checklistId, HotspotStub hotspot, Integer numberOfObservers, String groupId,
			LocalDate observationDate, LocalTime startTime, boolean isComplete, Double effortDistanceKm,
			Double effortAreaHa, Integer duration, String protocolType, String protocolCode) {
		super(checklistId);
		this.hotspot = hotspot;
		this.numberOfObservers = numberOfObservers;
		this.groupId = groupId;
		this.observationDate = observationDate;
		this.startTime = startTime;
		this.isComplete = isComplete;
		this.effortDistanceKm = effortDistanceKm;
		this.effortAreaHa = effortAreaHa;
		this.duration = duration;
		this.protocolType = protocolType;
		this.protocolCode = protocolCode;
	}
	
	public Checklist insert(final Connection connection) throws SQLException {
		try(final PreparedStatement ps = connection.prepareStatement("INSERT INTO checklist (checklist_id, hotspot, observer_count, group_id, date, time_started, effort_distance, effort_ha, duration, protocol_type, protocol_code, is_complete) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
			ps.setString(1, getChecklistId());
			ps.setString(2, hotspot.getLocalityId());
			ps.setObject(3, numberOfObservers);
			ps.setString(4, groupId);
			ps.setObject(5, observationDate, java.sql.Types.DATE);
			ps.setString(6, startTime == null? null : startTime.format(DateTimeFormatter.ISO_LOCAL_TIME));
			ps.setObject(7, effortDistanceKm);
			ps.setObject(8, effortAreaHa);
			ps.setObject(9, duration);
			ps.setString(10, protocolType);
			ps.setString(11, protocolCode);
			ps.setBoolean(12, isComplete);
			
			ps.executeUpdate();
			
			return this;
		}
	}
	
	public static Optional<Checklist> load(final Connection connection, final String checklistId) throws SQLException {
		try (final PreparedStatement ps = connection.prepareStatement("SELECT hotspot, observer_count, group_id, date, time_started, effort_distance, effort_ha, duration, protocol_type, protocol_code, is_complete FROM checklist WHERE checklist_id = ?")) {
			ps.setString(1, checklistId);
			
			final ResultSet rs = ps.executeQuery();
			
			if (rs.next()) {
				return Optional.of(
						new Checklist(
								checklistId,
								new HotspotStub(rs.getString("hotspot")),
								rs.getObject("observer_count", Integer.class),
								rs.getString("group_id"),
								LocalDate.parse(rs.getString("date"), DateTimeFormatter.ISO_LOCAL_DATE),
								toTime(rs.getString("time_started")),
								rs.getBoolean("is_complete"),
								rs.getDouble("effort_distance"),
								rs.getDouble("effort_ha"),
								rs.getInt("duration"),
								rs.getString("protocol_type"),
								rs.getString("protocol_code")
						)
				);
			} else {
				return Optional.empty();
			}
		}
	}
	
	private static LocalTime toTime(final String time) {
		if (time == null) {
			return null;
		}
		
		return LocalTime.parse(time, DateTimeFormatter.ISO_LOCAL_TIME);
	}
}
