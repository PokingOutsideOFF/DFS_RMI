import java.io.Serializable;
import java.util.Objects;


public class ReplicaLoc implements Serializable{
	
	private static final long serialVersionUID = -4113307750760738108L;
	
	private String address;
	private int id;
	private boolean isAlive;
	private boolean isPrimary;

	
	public ReplicaLoc(int id, String address, boolean isAlive) {
		this.id = id;
		this.address = address;
		this.isAlive = isAlive;
		this.isPrimary = false;
	}
	
	boolean isAlive(){
		return isAlive;
	}
	
	int getId(){
		return id;
	}
	
	void setAlive(boolean isAlive){
		this.isAlive = isAlive;
	}
	
	String getAddress(){
		return address;
	}

	public boolean isPrimary() {
        return isPrimary;
    }

    public void setPrimary(boolean primary) {
        isPrimary = primary;
    }


	public String toString() {
        return "Replica" + id;
    }

    // Override equals() and hashCode() for proper comparison and hashing
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReplicaLoc that = (ReplicaLoc) o;
        return id == that.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
	}

	
}

