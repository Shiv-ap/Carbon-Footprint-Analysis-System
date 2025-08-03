SELECT 
    ac.category_name,
    ac.carbon_factor,
    AVG(da.quantity) as avg_quantity,
    AVG(da.quantity) * ac.carbon_factor as potential_reduction,
    COUNT(*) as frequency,
    'Reduce ' || ac.category_name || ' usage by 20%' as suggestion
FROM daily_activities da
JOIN activity_categories ac ON da.category_id = ac.category_id
GROUP BY ac.category_name, ac.carbon_factor
HAVING potential_reduction > 1
ORDER BY potential_reduction DESC;