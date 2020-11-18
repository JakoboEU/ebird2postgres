package ebird2postgres.ebird;

public interface EBirdLocalityPredicate {
	boolean accept(String localityId);
}
