SELECT d.*, count(*) 
FROM fire_incidents as i 
INNER JOIN district_info as d 
ON i.district_id = d.ID 
WHERE DATETIME('now')-incident_datetime <365 
GROUP BY D.incident_borough 

SELECT avg(engines_assigned_quantity), c.incident_classification 
FROM fire_incidents as i
INNER JOIN classifications as c
ON i.incident_classification = c.ID
GROUP BY c.ID

SELECT avg(incident_close_datetime-incident_datetime), d.policeprecinct
FROM fire_incidents as i 
INNER JOIN district_info as d 
ON i.district_id = d.ID 
GROUP BY d.policeprecinct

SELECT avg(incident_close_datetime-incident_datetime), cg.incident_classification_group
FROM fire_incidents as i 
INNER JOIN classifications as c
ON i.incident_classification = c.ID
INNER JOIN classification_groups
ON c.incident_classification_group = cg.ID
GROUP BY cg.incident_classification_group