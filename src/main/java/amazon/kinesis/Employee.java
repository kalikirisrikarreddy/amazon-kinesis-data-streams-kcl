package amazon.kinesis;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class Employee {
	private String id;
	private String name;
	private String designation;
	private String company;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getDesignation() {
		return designation;
	}

	public void setDesignation(String designation) {
		this.designation = designation;
	}

	public String getCompany() {
		return company;
	}

	public void setCompany(String company) {
		this.company = company;
	}

	public Employee() {
		super();
	}

	public Employee(String id, String name, String designation, String company) {
		super();
		this.id = id;
		this.name = name;
		this.designation = designation;
		this.company = company;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}

		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		Employee employee = (Employee) o;

		return new EqualsBuilder().append(getId(), employee.getId())
				.append(getName(), employee.getName()).append(getDesignation(), employee.getDesignation())
				.append(getCompany(), employee.getCompany()).isEquals();
	}

	@Override
	public int hashCode() {
		return new HashCodeBuilder(17, 37).append(getId()).append(getName()).append(getDesignation())
				.append(getCompany()).toHashCode();
	}

	@Override
	public String toString() {
		return "Employee{" +
				"id='" + id + '\'' +
				", name='" + name + '\'' +
				", designation='" + designation + '\'' +
				", company='" + company + '\'' +
				'}';
	}
}
