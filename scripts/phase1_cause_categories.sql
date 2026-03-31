-- ================================================================
-- PowerDonor.ai — Phase 1 Fix 5: Cause Categories Lookup Table
-- Run as: postgres (superuser)
--
-- Creates canonical cause_categories table with alias mappings.
-- All llm_cause_categories normalization references this table.
-- To add/edit categories: INSERT/UPDATE cause_categories only.
-- No code changes needed.
--
-- Category groups:
--   core        — main domestic cause areas
--   standalone  — identity/equity categories (confirm with founder)
--   international — regional breakdown (confirm with founder)
-- ================================================================

-- ----------------------------------------------------------------
-- STEP 1: Create cause_categories table
-- ----------------------------------------------------------------

CREATE TABLE IF NOT EXISTS cause_categories (
    id              SERIAL PRIMARY KEY,
    canonical_name  TEXT NOT NULL UNIQUE,
    category_group  TEXT NOT NULL DEFAULT 'core',  -- core, standalone, international
    aliases         TEXT[] NOT NULL DEFAULT '{}'
);

-- ----------------------------------------------------------------
-- STEP 2: Seed canonical categories + aliases
-- ----------------------------------------------------------------

INSERT INTO cause_categories (canonical_name, category_group, aliases) VALUES

-- ----------------------------------------------------------------
-- CORE CATEGORIES
-- ----------------------------------------------------------------
('Education', 'core', ARRAY[
    'Education', 'Higher Education', 'K-12 Education', 'Special Education',
    'Education and Training', 'Education & Training', 'Youth Education',
    'Early Childhood Education', 'Early Childhood Development',
    'Adult Education', 'Adult Literacy', 'Literacy', 'Libraries',
    'Library Services', 'Scholarships', 'Scholarships & Financial Aid',
    'Scholarships and Financial Aid', 'College Access', 'College Preparation',
    'STEM Education', 'STEM/STEAM', 'Music Education', 'Arts Education',
    'Religious Education', 'Christian Education', 'Character Education',
    'Professional Education', 'Medical Education', 'Trade Education',
    'Vocational Education', 'Language Education', 'Language Learning',
    'Education Support', 'Education & Workforce Development',
    'Education and Workforce Development', 'Community Education',
    'Youth & Education', 'Youth and Education', 'Teacher Development',
    'Student Support', 'Student Support Services', 'Student Services',
    'Student Development', 'Academic Research', 'Holocaust Education',
    'Environmental Education', 'Health Education', 'Financial Education',
    'Financial Literacy', 'Safety Education', 'Business Education',
    'Industry Education', 'Legal Education', 'Outdoor Education',
    'Character Development', 'Character Building', 'Leadership',
    'Leadership Development', 'Leadership Training'
]),

('Health & Medicine', 'core', ARRAY[
    'Health', 'Healthcare', 'Health Care', 'Health & Wellness',
    'Health and Wellness', 'Health Services', 'Health & Medical',
    'Health & Medical Care', 'Health & Medical Services',
    'Health & Medical Research', 'Health & Medical Support',
    'Health and Medical Research', 'Health/Medical Research',
    'Healthcare Services', 'Healthcare Access', 'Healthcare Advocacy',
    'Healthcare Innovation', 'Healthcare Policy', 'Healthcare Quality',
    'Healthcare Technology', 'Healthcare Support', 'Healthcare Education',
    'Medical Care', 'Medical Research', 'Medical Services',
    'Medical Training', 'Medical Education', 'Medical Support',
    'Medical Assistance', 'Medical Advocacy', 'Primary Care',
    'Preventive Care', 'Preventive Medicine', 'Patient Support',
    'Patient Support Services', 'Patient Care', 'Patient Advocacy',
    'Patient Safety', 'Patient Education', 'Hospital Services',
    'Hospital Care', 'Hospice Care', 'Hospice and Palliative Care',
    'Hospice and End-of-Life Care', 'End-of-Life Care', 'Long-term Care',
    'Long-Term Care', 'Palliative Care', 'Dental Care', 'Dental Health',
    'Vision Care', 'Pediatric Care', 'Pediatric Health',
    'Maternal Health', 'Maternal and Child Health', 'Maternal & Child Health',
    'Women''s Health', 'Children''s Health', 'Rural Health', 'Rural Healthcare',
    'Community Health', 'Community Health Services', 'Public Health',
    'Global Health', 'Cancer', 'Cancer Support', 'Cancer Research',
    'Cancer Care', 'Cancer Prevention', 'HIV/AIDS', 'HIV/AIDS Support',
    'Rare Diseases', 'Rare Disease', 'Disease Prevention', 'Disease Research',
    'Disease Support', 'Diseases and Conditions', 'Genetic Disorders',
    'Neurological Disorders', 'Developmental Disabilities',
    'Nursing', 'Rehabilitation', 'Rehabilitation Services',
    'Emergency Medicine', 'Emergency Medical Services',
    'Occupational Health and Safety', 'Health & Safety',
    'Health and Safety', 'Health & Fitness', 'Health & Nutrition',
    'Health and Nutrition', 'Health & Human Services',
    'Health and Human Services', 'Health and Social Services',
    'Health & Social Services', 'Health & Mental Health',
    'Health and Mental Health', 'Health & Medicine',
    'Health & Healthcare', 'Health and Well-being', 'Health and Welfare',
    'Wellness', 'Fitness', 'Physical Fitness', 'Fitness and Wellness',
    'Biotechnology', 'Veterinary Services', 'Veterinary Care',
    'Veterinary Medicine', 'Animal Health', 'Alternative Medicine',
    'Nutrition', 'Food Safety', 'Water and Sanitation', 'Water & Sanitation',
    'Water Safety', 'Water Quality', 'Water Protection', 'Water Resources',
    'Water Conservation', 'Water'
]),

('Mental Health & Counseling', 'core', ARRAY[
    'Mental Health', 'Mental Health & Wellness', 'Mental Health and Wellness',
    'Mental Health & Counseling', 'Mental Health and Counseling',
    'Mental Health Support', 'Mental Health & Support Services',
    'Mental Health & Crisis Support', 'Mental Health & Grief Support',
    'Mental Health & Behavioral Health', 'Mental Health and Addiction',
    'Mental Health & Addiction', 'Mental Health & Substance Abuse',
    'Mental Health and Substance Abuse', 'Mental Health/Counseling',
    'Mental Health & Mental Wellness', 'Behavioral Health',
    'Counseling', 'Counseling Services', 'Crisis Intervention',
    'Crisis Support', 'Crisis Services', 'Suicide Prevention',
    'Grief Support', 'Bereavement Support', 'Trauma Recovery',
    'Trauma Support', 'Autism', 'Autism Support', 'Learning Disabilities',
    'Intellectual and Developmental Disabilities'
]),

('Substance Abuse & Recovery', 'core', ARRAY[
    'Substance Abuse Treatment', 'Substance Abuse Prevention',
    'Substance Abuse & Recovery', 'Substance Abuse Recovery',
    'Substance Abuse & Addiction', 'Substance Abuse/Addiction',
    'Substance Abuse/Addiction Recovery', 'Substance Abuse Prevention and Treatment',
    'Substance Use Disorder', 'Substance Use Disorder Treatment',
    'Addiction Recovery', 'Addiction Treatment', 'Addiction Services',
    'Addiction & Recovery', 'Addiction and Recovery',
    'Addiction & Substance Abuse', 'Addiction and Substance Abuse',
    'Substance Abuse', 'Recovery'
]),

('Community Development', 'core', ARRAY[
    'Community Development', 'Community Service', 'Community Services',
    'Community Support', 'Community Building', 'Community Engagement',
    'Community Improvement', 'Community Outreach', 'Community Empowerment',
    'Community Enrichment', 'Community Promotion', 'Community Projects',
    'Community Advocacy', 'Community Organizing', 'Community Recreation',
    'Community Events', 'Community Education', 'Community Safety',
    'Community Health', 'Community Wellness', 'Community Support Services',
    'Community and Economic Development', 'Community development',
    'Neighborhood Services', 'Neighborhood Improvement',
    'Neighborhood Revitalization', 'Neighborhood Safety',
    'Urban Development', 'Urban Planning', 'Urban Revitalization',
    'Rural Development', 'Regional Development', 'Local Economy',
    'Local Economic Development', 'Local Business Support',
    'Infrastructure', 'Real Estate', 'Real Estate Industry',
    'Property Management', 'Construction', 'Architecture',
    'Architecture and Design', 'Urban Planning'
]),

('Youth & Children', 'core', ARRAY[
    'Youth Development', 'Youth Services', 'Children & Youth',
    'Youth Programs', 'Child Welfare', 'Children and Youth',
    'Youth & Education', 'Youth and Education', 'Youth & Children',
    'Youth and Children', 'Youth & Family Services', 'Youth and Family Services',
    'Youth & Families', 'Youth & Sports', 'Youth Sports',
    'Youth Leadership', 'Youth Engagement', 'Youth Empowerment',
    'Youth Support', 'Youth & Young Adults', 'Youth & Children Services',
    'Children & Youth Services', 'Children and Youth Services',
    'Children & Family Services', 'Children and Family Services',
    'Child & Family Services', 'Child Development', 'Child Care',
    'Child Care & Development', 'Child Care and Development',
    'Early Childhood Care', 'Childcare', 'Child Protection',
    'Child Abuse Prevention', 'Child Safety', 'Child Advocacy',
    'Child Welfare', 'Children''s Welfare', 'Children''s Services',
    'Children''s Health', 'Children', 'Foster Care', 'Foster Care & Adoption',
    'Foster Care and Adoption', 'Foster Care Support', 'Adoption',
    'Adoption Services', 'Adoption Support', 'Pet Adoption',
    'Mentoring', 'Mentorship', 'After School', 'Tutoring',
    'Girls Empowerment', 'Girls'' Empowerment', 'Orphan Care',
    'Special Education', 'Special Needs', 'Special Needs Support',
    'Youth Athletics', 'Athletic Development', 'Youth Programs',
    'K-12 Schools', 'K-12 Education'
]),

('Arts & Culture', 'core', ARRAY[
    'Arts & Culture', 'Arts and Culture', 'Cultural Arts', 'Arts',
    'Culture', 'Culture & Arts', 'Culture and Arts', 'Cultural Affairs',
    'Cultural Heritage', 'Cultural Preservation', 'Cultural Exchange',
    'Cultural Education', 'Cultural Understanding', 'Cultural Events',
    'Cultural Enrichment', 'Heritage Preservation', 'Historic Preservation',
    'Historical Preservation', 'Heritage Conservation', 'Heritage & History',
    'Heritage & Culture', 'Culture & Heritage', 'History & Heritage',
    'History & Culture', 'History and Heritage', 'History and Culture',
    'History & Preservation', 'History & Humanities', 'History',
    'Local History', 'Military History', 'Jewish Heritage', 'Jewish Life',
    'Jewish Life and Culture', 'Performing Arts', 'Music', 'Theater',
    'Theatre', 'Dance', 'Visual Arts', 'Literature', 'Film & Media',
    'Film and Media', 'Music & Arts', 'Music and Arts', 'Museums',
    'Libraries', 'Humanities', 'Genealogy', 'Archaeology',
    'Media & Broadcasting', 'Media and Broadcasting', 'Media',
    'Media & Communications', 'Media and Communications',
    'Media & Journalism', 'Media and Journalism', 'Journalism',
    'Publishing', 'Entertainment', 'Entertainment Industry',
    'Artist Support', 'Arts Education', 'Music Education'
]),

('Environment & Conservation', 'core', ARRAY[
    'Environment', 'Conservation', 'Environmental Conservation',
    'Environmental Sustainability', 'Environmental Protection',
    'Environmental Advocacy', 'Environmental Justice',
    'Environmental Management', 'Environmental Health',
    'Environmental Stewardship', 'Environmental Education',
    'Environment & Conservation', 'Environment/Conservation',
    'Environment and Conservation', 'Environment & Sustainability',
    'Climate Change', 'Climate Action', 'Climate Justice', 'Climate',
    'Climate & Environment', 'Sustainability', 'Sustainable Development',
    'Sustainable Agriculture', 'Wildlife Conservation', 'Wildlife Protection',
    'Wildlife', 'Wildlife Management', 'Animal Rights', 'Biodiversity',
    'Land Conservation', 'Land Trust', 'Land Protection', 'Land Preservation',
    'Marine Conservation', 'Ocean Conservation', 'Forestry',
    'Natural Resources', 'Water Resources', 'Habitat Restoration',
    'Renewable Energy', 'Clean Energy', 'Energy Efficiency',
    'Energy', 'Energy & Environment', 'Parks and Recreation',
    'Parks & Recreation', 'Outdoor Recreation', 'Outdoor Activities',
    'Horticulture', 'Agriculture', 'Local Agriculture', 'Local Food Systems',
    'Food Systems', 'Food & Agriculture', 'Waste Reduction',
    'Waste Management', 'Recycling', 'Water and Sanitation',
    'Water & Sanitation', 'Water Safety', 'Water Quality',
    'Water Protection', 'Water Conservation', 'Water'
]),

('Housing & Homelessness', 'core', ARRAY[
    'Housing', 'Homelessness', 'Housing & Homelessness',
    'Housing and Homelessness', 'Housing/Homelessness',
    'Homelessness & Housing', 'Poverty & Homelessness',
    'Poverty and Homelessness', 'Affordable Housing', 'Housing Assistance',
    'Housing Support', 'Housing & Shelter', 'Housing and Shelter',
    'Homeless Services', 'Homelessness Prevention', 'Shelter',
    'Rescue and Shelter', 'Rescue and Shelter Services',
    'Residential Services', 'Residential Care', 'Senior Living',
    'Independent Living', 'Public Housing', 'Fair Housing',
    'Real Estate', 'Property Management'
]),

('Social Services & Human Services', 'core', ARRAY[
    'Social Services', 'Human Services', 'Family Services',
    'Family Support', 'Family Support Services', 'Basic Needs',
    'Basic Needs Assistance', 'Social Welfare', 'General Charitable Purposes',
    'General Charitable Giving', 'General Philanthropy',
    'Charitable Services', 'Charitable Assistance', 'Charitable Giving',
    'charitable giving', 'Charity', 'Support Services',
    'Community Support Services', 'Social Impact', 'Social Change',
    'Social Enterprise', 'Social Responsibility', 'Social Equity',
    'Quality of Life', 'General Purposes', 'Humanitarian',
    'Humanitarian Services', 'Humanitarian Service',
    'Caregiver Support', 'Caregiving Support', 'Peer Support',
    'Crisis Support', 'Emergency Assistance', 'Financial Assistance',
    'Financial Stability', 'Financial Security', 'Financial Inclusion',
    'Financial Empowerment', 'Financial Wellness', 'Financial Aid',
    'Energy Assistance', 'Holiday assistance', 'Holiday Assistance',
    'Transportation', 'Transportation Safety', 'Utilities',
    'Public Utilities', 'Consumer Protection', 'Consumer Advocacy',
    'Consumer Rights', 'Information Access', 'Public Services',
    'Public Administration', 'Government Services', 'Local Government',
    'Government', 'Charitable Fundraising', 'Volunteerism',
    'Volunteer Services', 'Volunteer Service', 'Volunteering'
]),

('Economic Development & Employment', 'core', ARRAY[
    'Economic Development', 'Workforce Development', 'Employment',
    'Job Training', 'Career Development', 'Professional Development',
    'Small Business Support', 'Entrepreneurship', 'Business Support',
    'Business Development', 'Business & Economic Development',
    'Business and Economic Development', 'Economic Opportunity',
    'Economic Empowerment', 'Economic Justice', 'Economic Security',
    'Economic Mobility', 'Economic Support', 'Economic Policy',
    'Local Economic Development', 'Rural Development',
    'Tourism and Economic Development', 'Tourism', 'Tourism and Travel',
    'Small Business', 'Small Business Development',
    'Business & Entrepreneurship', 'Business and Entrepreneurship',
    'Business and Commerce', 'Business & Commerce', 'Business & Industry',
    'Business and Industry', 'Business and Trade', 'Business & Trade',
    'Business and Trade Association', 'Business/Trade Association',
    'Business and Professional Services', 'Business & Professional Services',
    'Business and Finance', 'Business & Economics', 'Business and Economics',
    'Business Ethics', 'Business Networking', 'Business Advocacy',
    'Industry Advocacy', 'Industry Support', 'Industry Association',
    'Industry Trade Association', 'Industry Standards', 'Industry Advancement',
    'Trade Association', 'Professional Association', 'Professional Associations',
    'Professional Organizations', 'Professional Networking',
    'Professional Certification', 'Professional Training',
    'Professional Services', 'Professional Advocacy',
    'Vocational Training', 'Vocational Rehabilitation', 'Skills Training',
    'Trade Skills Training', 'Skilled Trades', 'Skilled Trades Training',
    'Career Training', 'Career Services', 'Career Advancement',
    'Employment & Job Training', 'Employment Services', 'Employment Support',
    'Employment & Training', 'Employment and Training',
    'Employment & Economic Development', 'Employment and Economic Development',
    'Employment & Workforce Development', 'Employment and Workforce Development',
    'Employment & Career Development', 'Job Creation', 'Workforce Training',
    'Labor Rights', 'Labor Union', 'Labor Union Services', 'Labor Relations',
    'Labor and Employment', 'Labor & Employment', 'Labor',
    'Labor/Workers Rights', 'Labor and Workers'' Rights',
    'Labor & Workers'' Rights', 'Workers'' Rights', 'Worker Rights',
    'Worker Advocacy', 'Worker Safety', 'Worker Benefits', 'Worker Protection',
    'Workers'' Advocacy', 'Workers Rights', 'Workplace Safety',
    'Occupational Safety', 'Occupational Health and Safety',
    'Manufacturing', 'Construction Industry', 'Aviation',
    'Maritime Industry', 'Real Estate Industry', 'Insurance',
    'Insurance Industry', 'Telecommunications', 'Finance',
    'Financial Services', 'Retirement Security', 'Retirement Planning',
    'Employee Benefits', 'Trade Education', 'Trade and Commerce',
    'Trade Skills Training', 'Capacity Building', 'Organizational Development',
    'Nonprofit Support', 'Nonprofit Infrastructure', 'Nonprofits'
]),

('Food Security & Hunger Relief', 'core', ARRAY[
    'Food Security', 'Hunger Relief', 'Food Assistance',
    'Hunger & Food Security', 'Food & Hunger', 'Food Insecurity',
    'Hunger & Food Insecurity', 'Hunger/Food Security', 'Hunger',
    'Food and Hunger', 'Food Justice', 'Food Access',
    'Food & Nutrition', 'Food & Agriculture', 'Nutrition',
    'Local Food Systems', 'Food Systems', 'Food Safety'
]),

('Animal Welfare', 'core', ARRAY[
    'Animal Welfare', 'Animal Rescue', 'Animal Rights', 'Animal Protection',
    'Pet Adoption', 'Pet Rescue', 'Pet Health', 'Pet Rescue and Adoption',
    'Animal Rescue and Welfare', 'Animal Rescue and Adoption',
    'Animal-Assisted Therapy', 'Rescue and Adoption',
    'Rescue and Rehabilitation', 'Dog Rescue', 'Equine Care',
    'Livestock Industry', 'Animals', 'Breed Preservation'
]),

('Veterans & Military', 'core', ARRAY[
    'Veterans Services', 'Veterans Support', 'Military & Veterans',
    'Veterans & Military', 'Military Support', 'Veterans & Military Support',
    'Military & Veterans Support', 'Military and Veterans Support',
    'Military/Veterans Support', 'Military/Veterans',
    'Veterans & Military Families', 'Veterans', 'Veteran Services',
    'Veteran Support', 'Military Services', 'Military Family Support',
    'Military History', 'Military Heritage', 'Military and Veterans',
    'First Responders', 'First Responder Support', 'First Responders Support',
    'Law Enforcement', 'Law Enforcement Support', 'Fire Prevention',
    'Fire Protection', 'Patriotism', 'Patriotic Organizations',
    'National Security', 'Military & Veterans'
]),

('Human Rights & Social Justice', 'core', ARRAY[
    'Human Rights', 'Social Justice', 'Civil Rights', 'Civil Liberties',
    'Civil Rights & Social Justice', 'Equity', 'Equity & Inclusion',
    'Equity and Inclusion', 'Diversity and Inclusion', 'Diversity & Inclusion',
    'Diversity, Equity & Inclusion', 'Racial Equity',
    'Justice', 'Justice System', 'Law & Justice', 'Law and Justice',
    'Justice & Law', 'Justice and Legal Services',
    'Criminal Justice Reform', 'Criminal Justice', 'Criminal Justice & Reentry',
    'Criminal Justice/Reentry', 'Juvenile Justice', 'Reentry Services',
    'Reentry Support', 'Access to Justice', 'Voting Rights',
    'Democracy', 'Democracy and Governance', 'Democracy & Governance',
    'Democracy and Civic Engagement', 'Democracy & Civic Engagement',
    'Government Accountability', 'Government Reform', 'Governance',
    'Policy Advocacy', 'Policy & Advocacy', 'Policy and Advocacy',
    'Policy Research', 'Public Policy', 'Legislative Advocacy',
    'Political Advocacy', 'Political Engagement', 'Advocacy',
    'Advocacy and Policy', 'Advocacy & Policy', 'Consumer Rights',
    'Conflict Resolution', 'Peace and Conflict Resolution',
    'Peace & Conflict Resolution', 'Peace and Security', 'Peace',
    'Anti-Trafficking', 'Anti-Human Trafficking', 'Human Trafficking',
    'Human Trafficking Prevention', 'Sexual Violence Prevention',
    'Sexual Assault', 'Sexual Assault Services', 'Sexual Assault Support',
    'Sexual Assault Prevention', 'Violence Prevention',
    'Domestic Violence', 'Domestic Violence Prevention',
    'Domestic Violence Support', 'Abuse Prevention',
    'Child Abuse Prevention', 'Crime Prevention', 'Crime Victim Services',
    'Victim Services', 'Victim Support', 'Victim Support Services',
    'Victim Advocacy', 'Survivor Support', 'Gender Justice',
    'Gender Equity', 'Gender Equality', 'Pro-Life',
    'Reproductive Rights', 'Reproductive Health', 'Reproductive Justice',
    'Indigenous Rights', 'Indigenous Affairs', 'Indigenous Communities',
    'Native American Services', 'Native American/Indigenous Affairs',
    'Fair Trade', 'Civil and Human Rights', 'Economic Justice',
    'Social Equity', 'Social Change', 'Equity and Social Justice'
]),

('Disaster Relief & Emergency Services', 'core', ARRAY[
    'Disaster Relief', 'Emergency Services', 'Emergency Relief',
    'Emergency Assistance', 'Disaster Response', 'Disaster Preparedness',
    'Emergency Preparedness', 'Emergency Management', 'Emergency Response',
    'Emergency Services Support', 'Crisis Intervention', 'Crisis Services',
    'Crisis Support', 'Emergency Medical Services'
]),

('Research & Science', 'core', ARRAY[
    'Medical Research', 'Research', 'Scientific Research',
    'Science & Technology', 'Science and Technology', 'Science & Research',
    'Science and Research', 'Research & Science', 'Research & Development',
    'Research and Development', 'Research & Innovation',
    'Research and Innovation', 'Academic Research', 'Cancer Research',
    'Disease Research', 'Health & Medical Research',
    'Health and Medical Research', 'Health/Medical Research',
    'Engineering', 'Science', 'Biotechnology', 'Policy Research',
    'STEM', 'STEM/STEAM', 'STEM/Science', 'STEM Education'
]),

('Recreation & Sports', 'core', ARRAY[
    'Recreation', 'Sports', 'Youth Sports', 'Athletics',
    'Sports and Recreation', 'Sports & Recreation', 'Recreation & Sports',
    'Recreation and Sports', 'Recreation and Leisure', 'Recreation & Leisure',
    'Amateur Athletics', 'Sports and Athletics', 'Sports & Athletics',
    'Outdoor Recreation', 'Outdoor Activities', 'Parks and Recreation',
    'Parks & Recreation', 'Community Recreation', 'Youth Athletics',
    'Athletic Development', 'Physical Fitness', 'Fitness',
    'Fitness and Wellness', 'Health & Fitness', 'Martial Arts',
    'Soccer', 'Motorsports', 'Shooting Sports', 'Hobby and Recreation'
]),

('Disability Services', 'core', ARRAY[
    'Disability Services', 'Disabilities', 'Disability Support',
    'Disability Rights', 'Disabilities Services', 'Disabilities Support',
    'Disabilities Support', 'Special Needs', 'Special Needs Support',
    'Intellectual and Developmental Disabilities', 'Developmental Disabilities',
    'Learning Disabilities', 'Autism', 'Autism Support',
    'Rehabilitation', 'Rehabilitation Services', 'Independent Living',
    'Accessibility', 'Vision/Blindness', 'Hearing'
]),

('Aging & Senior Services', 'core', ARRAY[
    'Senior Services', 'Senior Care', 'Aging', 'Aging Services',
    'Elderly Care', 'Elder Care', 'Aging & Senior Services',
    'Aging and Senior Services', 'Aging & Seniors', 'Aging and Seniors',
    'Aging & Elderly Services', 'Aging and Elderly Services',
    'Aging & Elder Care', 'Aging and Elder Care', 'Seniors',
    'Seniors/Aging', 'Elderly Services', 'Senior Living',
    'Long-term Care', 'Long-Term Care', 'Residential Care',
    'End-of-Life Care', 'Hospice Care', 'Hospice and Palliative Care',
    'Retirement Security', 'Retirement Planning', 'Adult Services',
    'Lifelong Learning'
]),

('Immigration & Refugee Services', 'core', ARRAY[
    'Immigration', 'Refugee Services', 'Immigration Services',
    'Immigrant Services', 'Immigration & Refugee Services',
    'Immigration and Refugee Services', 'Refugee and Immigrant Services',
    'Refugee Support', 'Refugee', 'Immigration Rights',
    'Immigrant Rights', 'Immigration Support', 'Immigration Services'
]),

('Legal Services & Advocacy', 'core', ARRAY[
    'Legal Services', 'Legal Aid', 'Legal Advocacy',
    'Access to Justice', 'Advocacy', 'Policy Advocacy',
    'Legal Education', 'Justice and Legal Services',
    'Law & Justice', 'Law and Justice', 'Consumer Protection',
    'Consumer Advocacy', 'Government Relations', 'Public Affairs',
    'Government Affairs', 'Legislative Advocacy'
]),

('Philanthropy & Grantmaking', 'core', ARRAY[
    'Philanthropy', 'Grantmaking', 'Charitable Giving',
    'Philanthropy & Grantmaking', 'Philanthropy/Grantmaking',
    'Charitable Fundraising', 'Foundations', 'General Philanthropy',
    'Philanthropy, Voluntarism and Grantmaking Foundations',
    'Nonprofit Support', 'Capacity Building', 'Organizational Development',
    'General Charitable Purposes', 'General Charitable Giving',
    'Charitable Services', 'Charitable Assistance'
]),

('Public Safety', 'core', ARRAY[
    'Public Safety', 'Crime Prevention', 'Law Enforcement',
    'Law Enforcement Support', 'Fire Prevention', 'Fire Protection',
    'First Responders', 'First Responder Support', 'First Responders Support',
    'Safety', 'Safety Education', 'Workplace Safety', 'Worker Safety',
    'Occupational Safety', 'Transportation Safety', 'Water Safety',
    'Health & Safety', 'Health and Safety', 'Community Safety',
    'Neighborhood Safety', 'Emergency Services', 'Emergency Management',
    'National Security', 'Cybersecurity', 'Consumer Safety'
]),

('Technology & Innovation', 'core', ARRAY[
    'Technology', 'Technology & Innovation', 'Technology and Innovation',
    'Innovation', 'Research & Innovation', 'Science & Technology',
    'Science and Technology', 'STEM', 'STEM Education', 'STEM/STEAM',
    'Engineering', 'Biotechnology', 'Cybersecurity',
    'Technology Access', 'Technology for Good', 'Digital Access',
    'Open Data', 'Data Access', 'Telecommunications',
    'Business & Technology', 'Media & Technology'
]),

('Civic Engagement & Democracy', 'core', ARRAY[
    'Civic Engagement', 'Democracy', 'Voting Rights', 'Public Policy',
    'Government Accountability', 'Government Reform', 'Governance',
    'Democracy and Governance', 'Democracy & Governance',
    'Democracy and Civic Engagement', 'Democracy & Civic Engagement',
    'Political Engagement', 'Community Organizing', 'Volunteerism',
    'Volunteer Services', 'Volunteer Service', 'Volunteering',
    'Social Responsibility', 'Government & Civic Engagement',
    'Public Service', 'Public Affairs', 'Civil Society'
]),

-- ----------------------------------------------------------------
-- STANDALONE CATEGORIES (confirm with founder)
-- ----------------------------------------------------------------
('LGBTQ+ Rights & Services', 'standalone', ARRAY[
    'LGBTQ+ Rights', 'LGBTQ+ Support', 'LGBTQ+ Services',
    'LGBTQ Rights', 'LGBTQ+ Advocacy', 'LGBTQ+ Community',
    'LGBTQIA+ Rights'
]),

('Women''s Empowerment', 'standalone', ARRAY[
    'Women''s Empowerment', 'Women Empowerment', 'Women''s Services',
    'Women''s Rights', 'Women & Girls', 'Women and Girls',
    'Women''s Organizations', 'Women''s Issues', 'Women''s Safety',
    'Women''s Health', 'Girls Empowerment', 'Girls'' Empowerment',
    'Gender Equality', 'Gender Equity', 'Gender Justice'
]),

('Racial Justice & Equity', 'standalone', ARRAY[
    'Racial Justice', 'Racial Equity', 'Civil Rights',
    'Anti-Racism', 'Diversity and Inclusion', 'Diversity & Inclusion',
    'Equity and Inclusion', 'Equity & Inclusion',
    'Diversity, Equity & Inclusion', 'Social Justice',
    'Economic Justice', 'Economic Equity'
]),

-- ----------------------------------------------------------------
-- INTERNATIONAL CATEGORIES (confirm regional split with founder)
-- ----------------------------------------------------------------
('International Development & Aid', 'international', ARRAY[
    'International Development', 'International Aid', 'International Relief',
    'Global Development', 'International Service', 'International Exchange',
    'International Affairs', 'International Relations', 'International Trade',
    'International Humanitarian Aid', 'Global Health', 'Global Development',
    'International', 'Worldwide', 'Global'
]),

('Humanitarian Relief', 'international', ARRAY[
    'Humanitarian Aid', 'Humanitarian Relief', 'Humanitarian Service',
    'Humanitarian Services', 'Disaster Relief', 'Emergency Relief',
    'Crisis Relief', 'War Relief', 'Refugee Services',
    'Conflict Relief', 'Relief efforts', 'Foreign Aid',
    'Peacekeeping', 'Peace and Security'
]),

('Africa', 'international', ARRAY[
    'Africa', 'Sub-Saharan Africa', 'East Africa', 'West Africa',
    'Southern Africa', 'North Africa', 'Kenya', 'Nigeria', 'Ghana',
    'Uganda', 'Tanzania', 'Rwanda', 'Zambia', 'Zimbabwe', 'Malawi',
    'Mozambique', 'Senegal', 'Sudan', 'Ethiopia', 'Somalia', 'Congo',
    'Democratic Republic of Congo'
]),

('Asia & South Asia', 'international', ARRAY[
    'Asia', 'South Asia', 'Southeast Asia', 'East Asia',
    'India', 'Bangladesh', 'Pakistan', 'Nepal', 'Sri Lanka',
    'Cambodia', 'Vietnam', 'Philippines', 'Indonesia', 'Myanmar',
    'Afghanistan', 'Thailand', 'China', 'Japan', 'Korea'
]),

('Latin America & Caribbean', 'international', ARRAY[
    'Latin America', 'Caribbean', 'Central America', 'South America',
    'Mexico', 'Guatemala', 'Honduras', 'El Salvador', 'Nicaragua',
    'Costa Rica', 'Panama', 'Colombia', 'Peru', 'Brazil', 'Bolivia',
    'Ecuador', 'Venezuela', 'Haiti', 'Cuba', 'Dominican Republic',
    'Puerto Rico'
]),

('Middle East', 'international', ARRAY[
    'Middle East', 'Gaza', 'Palestine', 'Israel', 'Syria',
    'Yemen', 'Iraq', 'Libya', 'Lebanon', 'Jordan', 'Iran',
    'Saudi Arabia', 'Turkey', 'Egypt'
]),

('Faith-Based International & Missionary Work', 'international', ARRAY[
    'Missionary Work', 'Mission Trip', 'International Missions',
    'Faith-Based International', 'Missionary', 'Evangelism',
    'Church Planting', 'Discipleship', 'Christian Ministry',
    'Christian Education', 'Christianity', 'Missions',
    'Global Missions', 'Cross-Cultural Ministry'
])

ON CONFLICT (canonical_name) DO NOTHING;

-- ----------------------------------------------------------------
-- STEP 3: Verify
-- ----------------------------------------------------------------

SELECT category_group, COUNT(*) AS category_count
FROM cause_categories
GROUP BY category_group
ORDER BY category_group;

SELECT canonical_name, category_group, array_length(aliases, 1) AS alias_count
FROM cause_categories
ORDER BY category_group, canonical_name;
