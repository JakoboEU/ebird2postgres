package ebird2postgres.repository;

public class HotspotStub {
	private final String localityId;

	public HotspotStub(String localityId) {
		super();
		this.localityId = localityId.trim();
	}

	public String getLocalityId() {
		return localityId;
	}
}
